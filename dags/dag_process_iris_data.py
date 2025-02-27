
from datetime import datetime
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.operators.postgres import PostgresOperator
import sys
import urllib.request
import logging

from modules.iris_data.process_iris_data_set import fetch_iris_data
from modules.iris_data.process_iris_data_set import save_processed_data_to_s3

sys.path.insert(0,"/opt/airflow/dags/modules/healthcare_analysis")

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s:%(funcName)s:%(levelname)s:%(message)s')

with DAG(
    dag_id='process_iris_data',
    schedule_interval='@daily',
    start_date=datetime(2024,1,26),
    catchup=False
) as dag:

# 1. Truncate table in Postgres
    task_truncate_table = PostgresOperator(
        task_id='truncate_tgt_table',
        postgres_conn_id='postgres_localhost',
        sql="TRUNCATE TABLE iris_tgt"
    )
# 2. Save processed data to Postgres
    task_load_iris_data = PythonOperator(
        task_id='load_iris_data',
        python_callable=fetch_iris_data

    )
# 2. Save processed data to s3 bucket  
    task_save_data_to_s3 = PythonOperator(
        task_id='save_to_s3',
        python_callable=save_processed_data_to_s3
    )    
task_truncate_table >> task_load_iris_data >> task_save_data_to_s3