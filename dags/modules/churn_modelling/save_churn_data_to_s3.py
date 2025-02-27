import pandas as pd
from datetime import datetime
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.operators.postgres import PostgresOperator
import sys
import numpy as numpy
import csv
import urllib.request
import logging
from pandasql import sqldf
import io
import os
import string 


sys.path.insert(0,"/opt/airflow/dags/modules/churn_modelling")


logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s:%(funcName)s:%(levelname)s:%(message)s')

bucket='churn-modelling'
def save_processed_credit_score_to_s3(**kwargs):
    #Make connection to postgres
    pg_hook = PostgresHook(
    postgres_conn_id='postgres_localhost',
    schema='postgres'
    )
    pg_conn = pg_hook.get_conn()
    cursor = pg_conn.cursor()

    #Define SQL query
    query1 = 'SELECT * FROM churn_modelling_creditscore;'

    #Read data into pandas dataframe
    df1 = pd.read_sql(query1, pg_conn)

    #Save df1 to S3
    filename = 'creditscore'
    s3_hook = S3Hook(aws_conn_id='minio_conn')
    s3_hook.load_string(df1.to_csv(index=False),
                        '{0}.csv'.format(filename),
                        bucket_name=bucket,
                        replace=True)

def save_processed_exited_age_correlation_to_s3(**kwargs):
    #Make connection to postgres
    pg_hook = PostgresHook(
    postgres_conn_id='postgres_localhost',
    schema='postgres'
    )
    pg_conn = pg_hook.get_conn()
    cursor = pg_conn.cursor()

    #Define SQL query
    query2 = 'SELECT * FROM churn_modelling_exited_age_correlation;'
    
    #Read data into pandas dataframe
    df2 = pd.read_sql(query2, pg_conn)
    
    #Save df2 to S3
    filename = 'exited_age_correlation'
    s3_hook = S3Hook(aws_conn_id='minio_conn')
    s3_hook.load_string(df2.to_csv(index=False),
                        '{0}.csv'.format(filename),
                        bucket_name=bucket,
                        replace=True)
    
def save_processed_exited_salary_correlation_to_s3(**kwargs):
    #Make connection to postgres
    pg_hook = PostgresHook(
    postgres_conn_id='postgres_localhost',
    schema='postgres'
    )
    pg_conn = pg_hook.get_conn()
    cursor = pg_conn.cursor()

    #Define SQL query
    query3 = 'SELECT * FROM churn_modelling_exited_salary_correlation;'

    #Read data into pandas dataframe
    df3 = pd.read_sql(query3, pg_conn)

    #Save df3 to S3
    filename = 'exited_salary_correlation'
    s3_hook = S3Hook(aws_conn_id='minio_conn')
    s3_hook.load_string(df3.to_csv(index=False),
                        '{0}.csv'.format(filename),
                        bucket_name=bucket,
                        replace=True)


def save_to_s3_main():
    save_processed_credit_score_to_s3()
    save_processed_exited_salary_correlation_to_s3()
    save_processed_exited_age_correlation_to_s3()
    
if __name__ == '__main__':
    save_processed_credit_score_to_s3()
    save_processed_exited_salary_correlation_to_s3()
    save_processed_exited_age_correlation_to_s3()
   
    

















#     #Save dataframe to S3
#     s3_hook = S3Hook(aws_conn_id='minio_conn')
#     s3_hook.load_string(dfs3.to_csv(index=False),
#                         '{0}.csv'.format(filename),
#                         bucket_name=bucket,
#                         replace=True)
           
# with DAG(
#     dag_id='postgres_db_dag',
#     schedule_interval='@daily',
#     start_date=datetime(2024,1,26),
#     catchup=False
# ) as dag:

# # 1. Truncate table in Postgres
#     task_truncate_table = PostgresOperator(
#         task_id='truncate_tgt_table',
#         postgres_conn_id='postgres_localhost',
#         sql="TRUNCATE TABLE iris_tgt"
#     )
# # 2. Save processed data to Postgres
#     task_load_iris_data = PythonOperator(
#         task_id='load_iris_data',
#         python_callable=to_alchemy
#     )
# # 2. Save processed data to s3 bucket  
#     task_save_data_to_s3 = PythonOperator(
#         task_id='save_to_s3',
#         python_callable=save_processed_data_to_s3
#     )    