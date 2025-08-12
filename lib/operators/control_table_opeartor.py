from airflow.models import BaseOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.sdk import Variable

import json

class ControlTableOperator(BaseOperator):
    """
    Custom operator to fetch fcontrol table requests and return structured metadata.

    Returns:
        list[dict]: A list of requests with parsed additional_info and pre-initialized status maps.

    """

    template_fields = ("snowflake_conn_id","process_name","pulled_status")
    
    def __init__(self, snowflake_conn_id, process_name, pulled_status, **kwargs):
        super().__init__(**kwargs)
        self.snowflake_conn_id = snowflake_conn_id
        self.process_name = process_name
        self.pulled_status = pulled_status

    def execute(self, context):
        self.log.info("Fetching requests for process '%s' with statuses: %s", self.process_name, self.pulled_status)
        database = Variable.get("DB_ADMIN")
        schema = Variable.get("SCHEMA_DB_ADMIN")
        
        snf_hook = SnowflakeHook(self.snowflake_conn_id,database=database,schema=schema)
        snf_conn = snf_hook.get_conn()
        data = []
        statuses_placeholders = ",".join(["%s"] * len(self.pulled_status))
        query = f"""
                SELECT 
                    ID,
                    PROCESS_NAME,
                    ADDITIONAL_INFO 
                FROM PPCDMDB.ADMIN.CT_REQUEST_INFO 
                WHERE 
                    PROCESS_NAME = %s 
                    AND STATUS IN ({ statuses_placeholders })
                """
        
        with snf_conn.cursor() as cursor:
            cursor.execute(query, (self.process_name, *self.pulled_status))

            for id,process_name,additional_info in cursor.fetchall():
                info = json.loads(additional_info) if additional_info else {}
                data.append({
                        'request_id': id,
                        'process_name': process_name,
                        'additional_info': info,
                        'dates': info.get('dates'),
                        'status': {
                            "initial" : {'Completed': [], 'Failed': [], 'Untracked': []},
                            "final" : {'Completed': [], 'Failed': []}
                        },
                        'request_status': None
                })
        self.log.info("Retrieved %d rows", len(data))
        return data