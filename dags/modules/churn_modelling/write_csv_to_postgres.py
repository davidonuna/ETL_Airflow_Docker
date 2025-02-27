"""
Downloads the csv file from the URL. Creates a new table in the Postgres server.
Reads the file as a dataframe and inserts each record to the Postgres table. 
"""
import sys
import os
import traceback
import logging
import pandas as pd
from pandasql import sqldf
import urllib.request
from db import connect


AIRFLOW_HOME = os.getenv('AIRFLOW_HOME')

sys.path.insert(0,"/opt/airflow/dags/modules/churn_modelling")


logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s:%(funcName)s:%(levelname)s:%(message)s')


connection=connect()
   
def create_postgres_table():
    
    """
    Create the Postgres table with a desired schema
    """
    try:
        connection.execute("""CREATE TABLE IF NOT EXISTS churn_modelling (RowNumber INTEGER PRIMARY KEY, CustomerId INTEGER, 
        Surname VARCHAR(50), CreditScore INTEGER, Geography VARCHAR(50), Gender VARCHAR(20), Age INTEGER, 
        Tenure INTEGER, Balance FLOAT, NumOfProducts INTEGER, HasCrCard INTEGER, IsActiveMember INTEGER, EstimatedSalary FLOAT, Exited INTEGER)""")
        
        logging.info(' New table churn_modelling created successfully to postgres server')
    except:
        logging.warning(' Check if the table churn_modelling exists')


def write_to_postgres():
    """
    Create the dataframe and write to Postgres table if it doesn't already exist
    """
    df = pd.read_csv(AIRFLOW_HOME + '/dags/raw_data/Churn_Modelling.csv')

    """
    Using  churn_modelling table to test this call library
    """
    
    df.to_sql(
        'churn_modelling', 
        con=connection, 
        index=False, 
        if_exists='replace'
    )
    print("Data inserted into Churn_Modelling table")
    
def write_csv_to_postgres_main():
    create_postgres_table()
    write_to_postgres()
    connection.close()


if __name__ == '__main__':
    create_postgres_table()
    write_to_postgres()
    connection.close()
    
