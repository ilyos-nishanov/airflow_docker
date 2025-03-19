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
    dag_id='onl_app',
    default_args=default_args,
    description='A DAG that schedules onl_app update',
    schedule='13 23 * * *',
    start_date=datetime(2025, 3, 18),
    catchup=False,
    max_active_runs=1
) as dag:
   
    update = BashOperator(
        task_id='update_onl_app',
        bash_command='cd /opt/airflow/scripts && python /opt/airflow/scripts/onl_app.py'
    )
   
    update 
 