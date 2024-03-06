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
import subprocess, sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
import scripts.azure_connection as azc #import the azure_connection.py file
import scripts.functions as fnc #import the functions.py file

command = sys.argv[1:]

#constants
storage_account = ''
current_date = datetime.now().strftime('%Y-%m-%d')
current_month = datetime.now().strftime('%Y-%m')
dataset_id = '306c67'

default_args = {
    'owner': 'luiscopete',
    'retries': 3,
    'retry_delay': timedelta(minutes=3)}

# ----- DAG definition ----- #
with DAG(
      dag_id='SIMEM-ETL-Orchestrator',
      description='Orchestrator for the SIMEM ETL process',
      start_date=datetime(2024, 3, 10),
      schedule_interval='@daily'           
    ) as dg:
    extract_data_API = PythonOperator(
        task_id='Extract data from SIMEM API',
        python_callable = fnc.api_get_file(dataset_id=dataset_id, startdate='2046-12-01', enddate='2046-12-01'))  
    upload_raw_data = PythonOperator(
        task_id='Upload extracted RAW data to Data Lake',
        python_callable=azc.upload_file_to_data_lake(
            storage_account, 
            destination_folder = f'SIMEM/{current_month}', 
            process_folder = 'raw', 
            file_system= 'raw',
            local_file_name= f'{dataset_id}.json',
            dl_file_name= f'{dataset_id}_{current_date}.json'))
    
    extract_data_API >> upload_raw_data
