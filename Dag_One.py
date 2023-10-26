from airflow import DAG 
from airflow.operators.python_operator import PythonOperator 
from datetime import datetime 

#define function (task or tasks)
def greetings():
            print("HI WORLD! I AM MAHESH")

default_args={
    'owner': "Mahesh",
    'start_date': datetime(2023,10,20,),
    'retries':1,
}

dag=DAG(
        'Wishing_dag_one',
        schedule_interval="10 18 * * *",
        catchup=False,
        default_args=default_args,
        description="This wishing tag runs everyday at 6.00 PM",
)


wishtag_task=PythonOperator(
    task_id='greeting',
    python_callable=greetings,
    dag=dag,

)