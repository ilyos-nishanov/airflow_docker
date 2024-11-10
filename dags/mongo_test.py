from airflow import DAG
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.operators.python import PythonOperator
from datetime import datetime

def mongo_test():
    mongo_hook = MongoHook(mongo_conn_id='mongodb_default')
    client = mongo_hook.get_conn()
    db = client.get_database()  # Get the default database
    print("Collections:", db.list_collection_names())

with DAG(
    'mongo_connection_test',
    start_date=datetime(2023, 11, 6),
    schedule=None,
    catchup=False,
) as dag:

    test_mongo = PythonOperator(
        task_id='test_mongo_connection',
        python_callable=mongo_test
    )
