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
    'katm_333_loans',
    default_args=default_args,
    description='A DAG that schedules an existing script',
    schedule_interval='7 5 * * *',
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:

    run_script = BashOperator(
        task_id='run_my_script',
        bash_command='cd /opt/airflow/scripts && python /opt/airflow/scripts/katm_333_general_cbr_loans.py'
    )
    run_script