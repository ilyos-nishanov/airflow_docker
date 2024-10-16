from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator


default_args = {
    'owner':'admin',
    'retries':5,
    'retry_delay':timedelta(minutes=5)
}

def get_sklearn():
    import sklearn
    print(f"scikit-learn version:{sklearn.__version__}")

def get_matplotlib():
    import matplotlib
    print(f"matplotlib version:{matplotlib.__version__}")

with DAG(
    dag_id='dag_with_python_dependencies_v02',
    default_args=default_args,
    start_date= datetime(2024, 9, 1, 2),  #starting from sep the 1st 2:00AM
    schedule_interval='0 0 * * *',       #check out crontab.guru for expressions
) as dag:
    get_sklearn=PythonOperator(
        task_id='get_scikit_learn',
        python_callable=get_sklearn
    )
    get_matplotlib=PythonOperator(
        task_id='get_matplotlib',
        python_callable=get_matplotlib
    )

get_sklearn
get_matplotlib

get_sklearn >> get_matplotlib