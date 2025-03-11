#%%
import psycopg2
import pandas as pd
from airflow import DAG
from psycopg2 import sql
from pymongo import MongoClient

from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.providers.postgres.operators.postgres import PostgresOperator

#%% Define MongoDB, OracleDB, and SQL Server connections
MONGO_URI = "mongodb://127.0.0.1:27017/?directConnection=true&serverSelectionTimeoutMS=2000&appName=mongosh+2.3.3"
#%%
def extract_from_mongodb():
    mongo_hook = MongoHook(mongo_conn_id='mongodb_default')
    client = mongo_hook.get_conn()
    db = client.admin #db name
    data = db.restaurants.find()  # Fetch data
    # return data
    df = pd.DataFrame(list(data))  # Convert cursor to list, then to DataFrame
    return df
#%%
def transform_data(data):
    # Transformation logic here
    polished_data = data  # Placeholder for transformed data
    return polished_data
#%%
def load_to_postgres(data):
    conn = psycopg2.connect("dbname=postgres user=airflow password=airflow host=localhost port=5432")
    cursor = conn.cursor()

    # Create table if not exists with correct column name (_id)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS public.my_dw_table (
            _id SERIAL PRIMARY KEY,
            name VARCHAR(255),
            borough VARCHAR(255)
        )
    """)

    # Insert or Update data
    for _, row in data.iterrows():
        cursor.execute("""
            INSERT INTO my_dw_table (_id, name, borough)
            VALUES (%s, %s, %s)
            ON CONFLICT (_id) 
            DO UPDATE SET name = EXCLUDED.name, borough = EXCLUDED.borough
        """, (row['_id'], row['name'], row['borough']))

    # Commit the transaction
    conn.commit()
    conn.close()

#%%
default_args = {
    'owner':'admin',
    'retries':5,
    'retry_delay':timedelta(minutes=5)
}

with DAG(
    dag_id='dag_with_mongodb_v01',
    default_args=default_args,
    start_date= datetime(2024, 9, 1, 2),  #starting from sep the 1st 2:00AM
    schedule='@daily',       #check out crontab.guru for expressions
) as dag:
    
    extract_mongo = PythonOperator(
        task_id='extract_from_mongodb',
        python_callable=extract_from_mongodb
    )

    transform = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data,
        op_args=[extract_mongo.output]  # Use outputs as inputs where needed
    )

    load_sql = PythonOperator(
        task_id='load_to_postgres',
        python_callable=load_to_postgres,
        op_args=[transform.output]
    )

    # Define task dependencies
    extract_mongo >> transform >> load_sql


# %%
