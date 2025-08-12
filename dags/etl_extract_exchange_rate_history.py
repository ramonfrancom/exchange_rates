from airflow.decorators import dag, task
# from airflow.models import Variable
from airflow.sdk import Variable
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator 
from airflow.utils.email import send_email
from datetime import datetime, timedelta
from dotenv import load_dotenv

import requests
import json
import time

from lib.operators.control_table_opeartor import ControlTableOperator
from lib.snowflake_connector import get_snowflake_connection_with_airflow_conn

# from scripts.exchange_rates.extract.pull_exchange_rates_from_api import get_requests_from_ct_snowflake, filter_dates_requested_with_date_already_processed_ct_snowflake

load_dotenv()

def notify_email(context):
    distribution_list = Variable.get("EMAIL_DEBUG_LIST")
    if distribution_list == None:
        raise EnvironmentError('EMAIL_DEBUG_LIST value is not defined in environment file')

    subject = f"Airflow Failure: {context['task_instance'].task_id}"
    body = \
    f"""
    DAG: {context['dag'].dag_id}
    Task: {context['task_instance'].task_id}
    Execution Time: {context['start_date']}
    Log URL: {context['task_instance'].log_url}
    """
    send_email(distribution_list,subject,body)

@dag(
    dag_id="etl_extract_exchange_rate_history",
    description="ETL extract process for updating dashboards for exchange rate history.",
    tags=["ETL","Extract","Exchange_rates"],
    schedule=timedelta(days=1),
    start_date=datetime(2025,7,16),
    catchup=False,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
        "on_failure_callback":notify_email,
        "depends_on_past": False,
    }
)
def etl_extract_exchange_rate_history():
    
    @task.short_circuit(task_id="check_request")
    def check_requests(pending_requests):
        if len(pending_requests) <= 0:
            return False
        else:
            return pending_requests
    
    @task(task_id="filter_extracted_dates")
    def filter_extracted_dates(snowflake_conn_id,pending_requests):
        database = Variable.get("DB_ADMIN")
        schema = Variable.get("SCHEMA_DB_ADMIN")
        cursor = get_snowflake_connection_with_airflow_conn(snowflake_conn_id,database,schema)

        requested_dates = pending_requests['dates']
        print(requested_dates)
        cursor.execute(f"""
                            WITH date_list as (
                                SELECT 
                                    TO_DATE(value::String) AS requested_date
                                FROM TABLE(FLATTEN(input => PARSE_JSON('["{'","'.join(requested_dates)}"]')))
                            )
                            SELECT 
                                date_list.requested_date,
                                extracted_dates.STATUS
                            FROM date_list
                            LEFT JOIN CT_EXTRACTED_DATES extracted_dates
                                ON date_list.requested_date = extracted_dates.EXTRACT_DATE
                                and extracted_dates.PROCESS_NAME = 'Historical Exchange Data Reports for USD'
                            ;
                    """)
        
        
        for [date,status] in cursor.fetchall():
            status_str = str(status if status is not None else 'Untracked') 
            pending_requests['initial_status_by_date'][status_str].append(date.strftime('%Y-%m-%d'))

        return pending_requests
    
    @task(task_id="explode_requests_by_date")
    def explode_requests_by_date(enriched_requests):

        reformated_data = []
        
        for enriched_request in enriched_requests:
            for key,dates in enriched_request['initial_status_by_date'].items():
                for date in dates:
                    reformated_data.append({
                        'request_id': enriched_request['request_id'],
                        'date': date,
                        'initial_status': key,
                        'status' : None
                    })
        
        return reformated_data
    
    @task(task_id="fetch_exchange_rate_from_api")
    def fetch_exchange_rate_from_api(requests_by_date):
        if requests_by_date['initial_status'] == 'Completed':
            requests_by_date['status'] = 'Completed'
            return requests_by_date
        
        api_key = Variable.get("API_KEY")
        data_dir = Variable.get('DATA_DIR')
        err_dir = Variable.get('LOGS_ERROR_DIR')

        date = requests_by_date['date']

        http_base = "https://api.exchangerate.host/historical"

        http_complete = f"{http_base}?access_key={api_key}&date={date}"
        print(http_complete)

        file_path = f"{data_dir}/{date}.json"
        

        try:
            response = requests.get(http_complete)
            data = response.json() if response.status_code == 200 else {}
            success = data.get("success", False)
        except Exception:
            success = False

        if success:
            with open(file_path, "w") as file:
                json.dump(data, file)

        if not success:
            err_file_path = f"{err_dir}/{date}.err"
            with open(err_file_path, "w") as file:
                file.write(response.text)

        requests_by_date['status'] = 'Completed' if success else 'Failed'
        return requests_by_date
    
    @task(task_id="update_extracted_dates_table")
    def update_extracted_dates_table(snowflake_conn_id,api_results_per_date,**context):

        '''
        {
            'request_id': request_info['request_id'],
            'date': date,
            'initial_status': key,
            'status' : None
        }
        '''
        request_id = api_results_per_date['request_id']
        date = api_results_per_date['date']
        initial_status = api_results_per_date['initial_status']
        status = api_results_per_date['status']

        database = Variable.get("DB_ADMIN")
        schema = Variable.get("SCHEMA_DB_ADMIN")

        cursor = get_snowflake_connection_with_airflow_conn(snowflake_conn_id,database,schema)

        run_time = context['logical_date'].strftime('%Y-%m-%d %H:%M:%S')
        if initial_status == 'Untracked': # Add new record
            cursor.execute(f"""
                INSERT INTO CT_EXTRACTED_DATES
                SELECT
                    ID AS request_id,
                    PROCESS_NAME,
                    '{date}' AS EXTRACT_DATE,
                    '{status}',
                    1 AS RECORD_COUNT,
                    TS_REQUESTED,
                    '{run_time}'
                FROM CT_REQUEST_INFO
                WHERE ID = {request_id}
                ORDER BY ID DESC, EXTRACT_DATE ASC;
            """)
        else: # Update record
            cursor.execute(f"""
                UPDATE CT_EXTRACTED_DATES 
                    SET STATUS = '{status}'
                    {
                        f", TS_COMPLETED ='{run_time}'" if status == 'Completed' else ''
                    }
                WHERE 
                    REQUEST_ID={request_id} 
                    AND EXTRACT_DATE='{date}';
            """)

    @task(task_id="aggregate_request_statuses")
    def aggregate_request_statuses(enriched_requests, api_results_per_date,__barrier):
        '''
            {
                'request_id': request_info['request_id'],
                'date': date,
                'initial_status': key,
                'status' : None
            }

            {
                'request_id': id,
                'dates': json.loads(dates)['ADDITIONAL_INFO']['dates'],
                'status_by_date': {'Success': [], 'Failed': []},
                'initial_status_by_date': {'Success': [], 'Failed': [], 'None': []},
                'request_status': None
            }
        '''

        dict_requests = {
            req['request_id']: req for req in enriched_requests
        }

        for api_request in api_results_per_date:
            request_id = api_request['request_id']
            date = api_request['date']
            status = api_request['status']
            dict_requests[request_id]['status_by_date'][status].append(date)

        for request in dict_requests.values():
            failed_dates = request['status_by_date'].get('Failed', [])
            request['request_status'] = 'Completed' if not failed_dates else 'Failed'
        
        return list(dict_requests.values())
    
    @task(task_id="finalize_request_status")
    def finalize_request_status(snowflake_conn_id,aggregated_requests,**context):
        '''
            {
                'request_id': id,
                'dates': json.loads(dates)['ADDITIONAL_INFO']['dates'],
                'status_by_date': {'Success': [], 'Failed': []},
                'initial_status_by_date': {'Success': [], 'Failed': [], 'None': []},
                'request_status': None
            }
        '''
        
        run_time = context['logical_date'].strftime('%Y-%m-%d %H:%M:%S')

        request_id = aggregated_requests['request_id']
        status = aggregated_requests['request_status']
        
        database = Variable.get("DB_ADMIN")
        schema = Variable.get("SCHEMA_DB_ADMIN")

        cursor = get_snowflake_connection_with_airflow_conn(snowflake_conn_id,database,schema)
        cursor.execute(f"""
                UPDATE CT_REQUEST_INFO 
                    SET STATUS = '{status}'
                    {
                        f", TS_COMPLETED ='{run_time}'" if status=='Completed' else ''
                    }
                WHERE 
                    ID={request_id};
            """)


    snowflake_conn_id = 'snowflake_conn'
    pending_requests = ControlTableOperator(
        snowflake_conn_id = snowflake_conn_id,
        process_name = 'Historical Exchange Data Reports for USD',
        pulled_status = ['Requested','Failed'])
    processed_request = check_requests(pending_requests)
    enriched_requests = filter_extracted_dates.partial(snowflake_conn_id=snowflake_conn_id).expand(pending_requests=processed_request)
    requests_by_date = explode_requests_by_date(enriched_requests=enriched_requests)
    api_results_per_date = fetch_exchange_rate_from_api.expand(requests_by_date=requests_by_date)
    update_ct_extracts = update_extracted_dates_table.partial(snowflake_conn_id=snowflake_conn_id).expand(api_results_per_date=api_results_per_date)
    aggregated_requests = aggregate_request_statuses(enriched_requests,api_results_per_date,update_ct_extracts)
    final_results = finalize_request_status.partial(snowflake_conn_id=snowflake_conn_id).expand(aggregated_requests=aggregated_requests)
    


dag = etl_extract_exchange_rate_history()