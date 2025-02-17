from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# Default arguments for all tasks
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Create a new DAG instance
with DAG(
    dag_id='katm_077_schedule',
    default_args=default_args,
    description='A DAG that runs katm_077 scripts with interval',
    schedule='0 2 * * *',  # Start daily at 2:00 AM
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1
) as dag:

    # List of script names to be run
    scripts = [
        'katm_077_contingent_liabilities_load_2.py',
        'katm_077_contracts_2.py',
        'katm_077_open_contracts_2.py',
        'katm_077_overdue_principals.py',
        'katm_077_overdue_procents.py',
        'katm_077_overview.py',
        'katm_077_scoring_2.py'
    ]

    # Initialize the first task
    previous_task = None

    # Create tasks for each script
    for i, script_name in enumerate(scripts):
        run_script = BashOperator(
            task_id=f'run_{script_name}',
            bash_command=f'cd /opt/airflow/scripts && python /opt/airflow/scripts/{script_name}'
        )

        # Add a sleep task
        sleep_task = BashOperator(
            task_id=f'sleep_after_{script_name}',
            bash_command='sleep 20'  # adjust sleep in secs
        )

        # Chain tasks: run script -> sleep
        if previous_task:
            previous_task >> run_script

        run_script >> sleep_task

        # Update the previous task reference
        previous_task = sleep_task