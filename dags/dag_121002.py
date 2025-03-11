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
    schedule='27 1 * * *',
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1
) as dag:
    
    backup = BashOperator(
        task_id='backup',
        bash_command='cd /opt/airflow/scripts && python /opt/airflow/scripts/_121002_backup.py'
    )

    truncate = BashOperator(
        task_id='truncate',
        bash_command='cd /opt/airflow/scripts && python /opt/airflow/scripts/_121002_truncate.py'
    )

    insert = BashOperator(
        task_id='insert',
        bash_command='cd /opt/airflow/scripts && python /opt/airflow/scripts/_121002_insert.py',
        retries = 0
    )

    
    backup >> truncate >> insert