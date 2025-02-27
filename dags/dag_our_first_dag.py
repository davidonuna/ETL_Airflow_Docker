from airflow import DAG
from datetime import datetime,timedelta
from airflow.operators.bash import BashOperator

default_args={
    'owner':'david',
    'retries':5,
    'retry_delay':timedelta(minutes=2)
}
with DAG(
    dag_id='my_first_dag_v3',
    default_args=default_args,
    description='This is my first dag that I wrote',
    start_date=datetime(2023,11,25,2),
    schedule_interval='@daily'

) as dag:
    task1=BashOperator(
        task_id='first_task1',
        bash_command="echo hello world,this is the first task"
    )
    task2=BashOperator(
        task_id='second_task1',
        bash_command="echo hello world,this is the second task"
    )
    task3=BashOperator(
        task_id='third_task1',
        bash_command="echo hello world,this is the third task"
    )
    task1>>[task2,task3]

