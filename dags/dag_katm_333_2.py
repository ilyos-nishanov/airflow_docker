from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'depends_on_past': False  # Ensures independent execution for each task
}

# Define the DAG
with DAG(
    dag_id='katm_333_dag_2',
    default_args=default_args,
    description='Run katm_333 scripts with delays and schedule',
    schedule_interval='0 4 * * *', 
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=7  # Allows parallel executions
) as dag:

    # List of scripts to run
    scripts = [
        'katm_333_collaterals.py',
        'katm_333_general_cbr_loans.py',
        'katm_333_general_cbr_scores.py',
        'katm_333_guarantors.py',
        'katm_333_loans_overview.py',
        'katm_333_monthly_detail.py',
        'katm_333_payments.py'
    ]

    # Initial delay for each task (in seconds)
    initial_delay = 0
    delay_increment = 600  # 10-minute increments for staggered execution

    # Create tasks with incremental delays
    for idx, script in enumerate(scripts):
        delay = initial_delay + (idx * delay_increment)
        BashOperator(
            task_id=f"run_{script}",
            bash_command=f"sleep {delay} && cd /opt/airflow/scripts && python {script}",
            trigger_rule="all_done"  # Ensures tasks run independently
        )
