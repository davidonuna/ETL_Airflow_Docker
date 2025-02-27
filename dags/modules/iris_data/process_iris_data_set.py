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
import numpy as np
from db import connect
import csv
import urllib.request
import logging
from pandasql import sqldf
import io


sys.path.insert(0,"/opt/airflow/dags/modules/healthcare_analysis")


logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s:%(funcName)s:%(levelname)s:%(message)s')

connection=connect()

def fetch_iris_data():
    pg_hook = PostgresHook(
    postgres_conn_id='postgres_localhost',
    schema='postgres'
    )
    pg_conn = pg_hook.get_conn()
    cursor = pg_conn.cursor()
    #Define SQL query
    query = 'SELECT * FROM iris;'

    #Read data into pandas dataframe
    df = pd.read_sql(query, pg_conn)

    #Process dataframe into new format
    df = df[
        (df['iris_sepal_length'] > 5) &
        (df['iris_sepal_width'] == 3) &
        (df['iris_petal_length'] > 3) &
        (df['iris_petal_width'] == 1.5)
        ]
    df= df.drop('iris_id', axis=1)
    

    """
    Using a iris_tgt table to test this call library
    """
    # engine = create_engine(connect)
    df.to_sql(
        'iris_tgt', 
        con=connection,
        index=False, 
        if_exists='replace'
    )
    print("Data inserted into iris_tgt table")

filename = 'iris_cleaned_data'
bucket='machine-learning'
def save_processed_data_to_s3(**kwargs):
    #Make connection to postges
    pg_hook = PostgresHook(
    postgres_conn_id='postgres_localhost',
    schema='postgres'
    )
    pg_conn = pg_hook.get_conn()
    cursor = pg_conn.cursor()

    #Define SQL query
    query = 'SELECT * FROM iris_tgt;'

    #Read data into pandas dataframe
    dfs3 = pd.read_sql(query, pg_conn)

    #Save dataframe to S3
    s3_hook = S3Hook(aws_conn_id='minio_conn')
    s3_hook.load_string(dfs3.to_csv(index=False),
                        '{0}.csv'.format(filename),
                        bucket_name=bucket,
                        replace=True)
           
