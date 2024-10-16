

from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator



default_args = {
    'owner': 'admin',
    'retries':5,
    'retry_delay': timedelta(minutes=5)
}


def greet(ti):
    name=ti.xcom_pull(task_ids='get_name',key='name')
    age = ti.xcom_pull(task_ids='get_age',key='age')
    print(f"hello world, my name is {name} and i am {age} years old")

def get_name(ti):
    ti.xcom_push(key='name',value='Jerry')

def get_age(ti):
    ti.xcom_push(key='age',value=20)



with DAG(
    default_args=default_args,
    dag_id='first_dag_with_python_v6',
    description='Our fist dag using python operator',
    start_date=datetime(2024,9,28,2),
    schedule_interval='@daily'
)as dag:
    
    task1= PythonOperator(
        task_id ='greet',
        python_callable=greet,
        # op_kwargs={
        #     # 'name':'tom',
        #     'age':20
        # }
    )

    task2 = PythonOperator(
        task_id='get_name',
        python_callable=get_name
    )

    task3 = PythonOperator(
        task_id='get_age',
        python_callable=get_age
    )



    #set dependencies
    [task2,task3]>>task1