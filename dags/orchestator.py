from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator



default_args = {
    'owner': 'luiscopete',
    'retries': 3,
    'retry_delay': timedelta(minutes=3)}

def greet():
    print("Hello, world!")

with DAG(
      dag_id='dag_with_python_operator',
      description='My first DAG with PythonOperator',
      start_date=datetime(2024, 2, 28),
      schedule_interval='@daily',           
    ) as dg:
    task1 = PythonOperator(
        task_id='greet',
        python_callable=greet
    )
    task1