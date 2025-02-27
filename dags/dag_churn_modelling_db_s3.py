import os
import sys
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator

from modules.churn_modelling.save_churn_data_to_s3 import save_to_s3_main
from modules.churn_modelling.write_csv_to_postgres import write_csv_to_postgres_main
from modules.churn_modelling.write_df_to_postgres import write_df_to_postgres_main

sys.path.insert(0,"/opt/airflow/dags/modules/churn_modelling")


start_date = datetime(2023, 1, 1, 12, 10)

default_args = {
    'owner': 'david',
    'start_date': start_date,
    'retries': 1,
    'retry_delay': timedelta(seconds=5)
}

with DAG('churn_modelling_db_to_s3', default_args=default_args, schedule_interval='@daily', catchup=False) as dag:

    write_csv_to_postgres = PythonOperator(
        task_id='write_csv_to_postgres',
        python_callable=write_csv_to_postgres_main,
        retries=1,
        retry_delay=timedelta(seconds=15))

    write_df_to_postgres = PythonOperator(
        task_id='write_df_to_postgres',
        python_callable=write_df_to_postgres_main,
        retries=1,
        retry_delay=timedelta(seconds=15))

    #  Save credit_score data to s3 bucket  
    task_save_credit_score_data_to_s3 = PythonOperator(
        task_id='save_credit_score_to_s3',
        python_callable=save_to_s3_main,
        retries=1,
        retry_delay=timedelta(seconds=15)
    )
     #  Save exited_age_correlation data to s3 bucket  
    task_save_age_correlation_data_to_s3 = PythonOperator(
        task_id='save_age_correlation_to_s3',
        python_callable=save_to_s3_main,
        retries=1,
        retry_delay=timedelta(seconds=15)
    ) 
     #  Save exited_salary_correlation data to s3 bucket  
    task_salary_correlation_to_s3 = PythonOperator(
        task_id='salary_correlation_to_s3',
        python_callable=save_to_s3_main,
        retries=1,
        retry_delay=timedelta(seconds=15)
    )            
    write_csv_to_postgres >> write_df_to_postgres >> [task_save_credit_score_data_to_s3,
    task_save_age_correlation_data_to_s3,task_salary_correlation_to_s3]
    