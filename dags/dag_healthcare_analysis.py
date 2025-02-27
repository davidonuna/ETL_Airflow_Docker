import os
import sys
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator

sys.path.insert(0,"/opt/airflow/dags/modules/healthcare_analysis")

from modules.healthcare_analysis.write_csv_to_postgres import write_csv_to_postgres_main


start_date = datetime(2023, 1, 1, 12, 10)

default_args = {
    'owner': 'david',
    'start_date': start_date,
    'retries': 1,
    'retry_delay': timedelta(seconds=5)
}

with DAG('healthcare_analysis_to_s3', default_args=default_args, schedule_interval='@daily', catchup=False) as dag:

    write_csv_to_postgres = PythonOperator(
        task_id='write_csv_to_postgres',
        python_callable=write_csv_to_postgres_main,
        retries=1,
        retry_delay=timedelta(seconds=15)
    )       
    write_csv_to_postgres