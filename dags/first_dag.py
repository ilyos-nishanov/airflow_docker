from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator



default_args={
    'owner':'admin',
    'retries':5,
    'retry_delay': timedelta(minutes=2)
}

with DAG(
    dag_id='first_dag_v6',
    default_args=default_args,
    description='This is my first dag.',
    start_date= datetime(2024, 9, 28, 2),  #starting from sep the 28th 2:00AM
    schedule_interval='@daily'
) as dag:
    
    task1 = BashOperator(
        task_id='task_one',
        bash_command='echo hello world, this is task one.'
    )

    task2=BashOperator(
        task_id='task_two',
        bash_command='echo hello planet, this will be run after task one.'
    )

    task3=BashOperator(
        task_id='task_three',
        bash_command='echo hello earth, this will be run after task one.'
    )
# #Set task dependencies method 1
# task1.set_downstream(task2)
# task1.set_downstream(task3)

#Set task dependancies method 2
task1 >> task2
task2 >> task3

#Set task dependancies method 3
task1 >> [task2, task3]