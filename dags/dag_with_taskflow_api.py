from datetime import datetime,timedelta
from airflow.decorators import dag,task

default_args={
    'owner':'david',
    'retries':5,
    'retry_delay':timedelta(minutes=5)
}

@dag(dag_id='dag_with_task_flow_api_v01',
    default_args=default_args,
    start_date=datetime(2023,11,30),
    schedule_interval='@daily')

def hello_world_etl():

    @task()
    def get_name():
        return "Jerry"
        

    @task()
    def get_age():
        return 20    
    
    @task()
    def greet(name,age):
        print(f"Hello world! My name is {name}" 
            f" and I'm {age} years old")

    name=get_name()
    age=get_age()
    greet(name=name,age=age)

greet_dag=hello_world_etl()     