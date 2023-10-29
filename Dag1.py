from airflow import DAG 
from airflow.operators.python_operator import PythonOperator 
from datetime import datetime

def greetings():
    print("Hi World!, I am Mahesh")

default_args = {
    'owner': 'your_name',
    'retries': 1,
}

dag= DAG (
    'wish_dag',
    schedule_interval = None,
    start_date = datetime(2023, 10, 18),
    catchup=False
)

wishing_task= PythonOperator(
    task_id='greetings_task',
    python_callable=greetings,
    provide_context=True,
    dag=dag
)