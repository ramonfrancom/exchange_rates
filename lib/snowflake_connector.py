import snowflake.connector 
from dotenv import load_dotenv
from airflow.hooks.base import BaseHook

def get_snowflake_connection(snf_user,snf_password,snf_account):
    conn = snowflake.connector.connect(
        user=snf_user,
        password=snf_password,
        account=snf_account,
        database="PPNCSRC",
        schema="ADMIN"
    )
    cursor = conn.cursor()
    return cursor

def get_snowflake_connection_with_airflow_conn(conn_id, database, schema):
    conn_info = BaseHook.get_connection(conn_id)
    conn = snowflake.connector.connect(
        user=conn_info.login,
        password=conn_info.password,
        account=conn_info.extra_dejson["account"],
        database=database,
        schema=schema
    )
    cursor = conn.cursor()
    return cursor
    