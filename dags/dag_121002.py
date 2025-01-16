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
    dag_id='121002',
    default_args=default_args,
    description='A DAG that schedules 121002 update',
    schedule_interval='27 1 * * *',
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1
) as dag:

    truncate = BashOperator(
        task_id='truncate',
        bash_command='cd /opt/airflow/scripts && python /opt/airflow/scripts/truncate_121002.py'
    )

    insert = BashOperator(
        task_id='insert',
        bash_command='cd /opt/airflow/scripts && python /opt/airflow/scripts/insert_121002.py',
        retries = 0
    )

    
    truncate >> insert