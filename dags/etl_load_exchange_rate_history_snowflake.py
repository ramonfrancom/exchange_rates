from airflow.decorators import dag, task
from airflow.sdk import Variable

from lib.snowflake_connector import get_snowflake_connection_with_airflow_conn
from pathlib import Path

@dag(
    dag_id="etl_load_exchange_rate_history_snowflake",
    description="ETL load process files to snowflake"
)
def etl_load_exchange_rate_history_snowflake():

    @task(task_id="load_files_to_snowflake")
    def load_files_to_snowflake(snowflake_conn_id):

        data_dir = Variable.get("DATA_DIR")

        data_path = Path(f'{data_dir}/')
        if not any(data_path.glob("*.json")):
            print("No files where found in data folder")
            return
        
        cursor = get_snowflake_connection_with_airflow_conn(snowflake_conn_id)

        # cursor.execute("CREATE TABLE PPNCSRC.ADMIN.test (name VARCHAR(30),edad NUMBER(3,0));")

        cursor.execute(f"PUT 'file://{data_dir}/*.json' @fx_stage AUTO_COMPRESS=FALSE")
        cursor.execute("""
            COPY INTO fx_raw(filename,last_modified,data)
            FROM (
                SELECT 
                    METADATA$FILENAME,
                    METADATA$FILE_LAST_MODIFIED,
                    $1
                FROM @fx_stage 
            )
            FILE_FORMAT=(TYPE='JSON')
        """)

    snowflake_conn_id = 'snowflake_conn'
    load_data = load_files_to_snowflake(snowflake_conn_id)

dag = etl_load_exchange_rate_history_snowflake()