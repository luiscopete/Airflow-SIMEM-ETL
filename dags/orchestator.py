#######################################################
# Developer: Luis Copete                              #
# Role: Data Engineer                                 #
# Description: Orchestrator for the SIMEM ETL process #
#######################################################

#libraries
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

#Functions
def api_request(dataset_id, startdate, enddate):
    bash_command = f'bash ../API_Request.sh {dataset_id} {startdate} {enddate}'
    return bash_command

default_args = {
    'owner': 'luiscopete',
    'retries': 3,
    'retry_delay': timedelta(minutes=3)}

def greet():
    print("Hello, world!")

#DAG
with DAG(
      dag_id='SIMEM-ETL-Orchestrator',
      description='Orchestrator for the SIMEM ETL process',
      start_date=datetime(2024, 2, 28),
      schedule_interval='@daily'           
    ) as dg:
    extract_data_API = BashOperator(
        task_id='extract_data_API',
        bash_command= api_request('306c67', '2046-12-01', '2046-12-01')
    )
    extract_data_API
