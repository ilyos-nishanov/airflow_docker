from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Create a new DAG instance
with DAG(
    'katm_333_payments',
    default_args=default_args,
    description='A DAG that schedules an existing script',
    schedule_interval='47 6 * * *',
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:

    run_script = BashOperator(
        task_id='run_my_script',
        bash_command='cd /opt/airflow/scripts && python /opt/airflow/scripts/katm_333_payments.py'
    )
    run_script
