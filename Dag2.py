from airflow import DAG 
from airflow.operators.python_operator import PythonOperator 
from datetime import datetime 

#define function (task or tasks)
def greetings():
            print("HI WORLD! I AM MAHESH")

default_args={
    'owner': "Mahesh",
    'start_date': datetime(2023,10,18,6,58,0),
    'retries':1,
}

dag=DAG(
        'Wishing_dag',
        schedule_interval="19 30 * * *",
        catchup=False,
        default_args=default_args,
        description="This wishing tag runs everyday at 7.30 PM",
)

wishtag_task=PythonOperator(
    task_id='greeting',
    python_callable=greetings,
    dag=dag,

)