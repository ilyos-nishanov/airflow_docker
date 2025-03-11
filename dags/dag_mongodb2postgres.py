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
    'dag_mongodb2postgres',
    default_args=default_args,
    description='A DAG that schedules an existing script',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:

    run_script = BashOperator(
        task_id='run_my_script',
        bash_command='python /opt/airflow/scripts/mongodb2postgres.py'
    )
    run_script
