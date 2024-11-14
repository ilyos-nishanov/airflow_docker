from airflow import DAG
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.providers.oracle.hooks.oracle import OracleHook
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

def extract_from_mongo():
    mongo_hook = MongoHook(mongo_conn_id="mongo_default")
    # Connect to MongoDB and extract data
    mongo_client = mongo_hook.get_conn()
    db = mongo_client['your_db']
    collection = db['your_collection']
    data = list(collection.find())
    return data

def transform_and_load_to_oracle(data):
    oracle_hook = OracleHook(oracle_conn_id="oracle_default")
    # Transform data and load it into Oracle DB
    connection = oracle_hook.get_conn()
    cursor = connection.cursor()
    for item in data:
        cursor.execute("INSERT INTO your_table (column1, column2) VALUES (:1, :2)", (item['field1'], item['field2']))
    connection.commit()

def load_to_sql_server(data):
    mssql_hook = MsSqlHook(mssql_conn_id="mssql_default")
    # Load data into SQL Server
    connection = mssql_hook.get_conn()
    cursor = connection.cursor()
    for item in data:
        cursor.execute("INSERT INTO your_sql_server_table (column1, column2) VALUES (?, ?)", (item['field1'], item['field2']))
    connection.commit()

with DAG('data_movement_dag', start_date=datetime(2024, 11, 1), schedule_interval='@daily') as dag:
    data_from_mongo = PythonOperator(
        task_id='extract_from_mongo',
        python_callable=extract_from_mongo
    )

    transform_load_oracle = PythonOperator(
        task_id='transform_and_load_to_oracle',
        python_callable=transform_and_load_to_oracle,
        op_args=[data_from_mongo.output]
    )

    load_to_sql = PythonOperator(
        task_id='load_to_sql_server',
        python_callable=load_to_sql_server,
        op_args=[transform_load_oracle.output]
    )

