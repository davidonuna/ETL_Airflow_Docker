"""
Downloads the csv file from the URL. Creates a new table in the Postgres server.
Reads the file as a dataframe and inserts each record to the Postgres table. 
"""
import psycopg2
import os
import sys
import traceback
import logging
import pandas as pd
import urllib.request
from db import connect

AIRFLOW_HOME = os.getenv('AIRFLOW_HOME')

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s:%(funcName)s:%(levelname)s:%(message)s')


connection = connect()




def write_csv_to_postgres():
    """
    Create the dataframe and write to Postgres table if it doesn't already exist
    """
    df1 = pd.read_csv(AIRFLOW_HOME + '/dags/raw_data/Patient_notes_cleaned.csv')
    

    """
    Using  encounters table to test this call library
    """
    df1.to_sql(
        'nch_patient_notes table', 
        con=connection, 
        index=False, 
        if_exists='replace'
    )
    print("Data inserted into nch_patient_notes table")

    
    
def write_csv_to_postgres_main():
    write_csv_to_postgres()
    # write_to_postgres()
    # cur.close()
    # connection.close()

if __name__ == '__main__':
    write_csv_to_postgres()
    # write_to_postgres()
    cur.close()
    connection.close()
