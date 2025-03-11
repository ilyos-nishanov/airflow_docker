from airflow import DAG
from airflow.operators.bash import BashOperator

from datetime import datetime, timedelta

default_args = {
    'owner':'admin',
    'retries':5,
    'retry_delay':timedelta(minutes=5)
}

with DAG(
    dag_id='dag_with_cron_expression_v03',
    default_args=default_args,
    start_date= datetime(2024, 9, 1, 2),  #starting from sep the 1st 2:00AM
    schedule_interval='0 3 * * Tue,Fri',       #check out crontab.guru for expressions
) as dag:
    task1 =BashOperator(
        task_id = 'task1',
        bash_command='echo dag with cron expression'
    )