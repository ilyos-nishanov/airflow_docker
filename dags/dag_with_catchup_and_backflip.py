from airflow import DAG
from airflow.operators.bash import BashOperator

from datetime import datetime, timedelta

default_args = {
    'owner':'admin',
    'retries':5,
    'retry_delay':timedelta(minutes=5)
}

with DAG(
    dag_id='dag_with_catchup_backflip_v01',
    default_args=default_args,
    description='This is my first dag.',
    start_date= datetime(2024, 9, 28, 2),  #starting from sep the 28th 2:00AM
    schedule_interval='@daily',
    catchup=False
) as dag:
    task1 =BashOperator(
        task_id = 'task1',
        bash_command='echo This is a simple bash command'
    )