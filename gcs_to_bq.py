from airflow import DAG
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from datetime import datetime

# Define default_args as a dictionary, not a set
default_args = {
    'owner': "Mahesh",
    'start_date': datetime(2023, 10, 25),  # Corrected the date format
    'retries': 1,
}

dag = DAG(
    'data_move_from_gcs_to_bq',  # Corrected the DAG name
    schedule_interval='@daily',
    catchup=False,
    default_args=default_args,
    description="file move from GCS to BigQuery everyday",
)

gcs_to_bq_task = GCSToBigQueryOperator(
    task_id="load_data_gcs_to_bq",
    bucket='dagbkt',
    source_objects=['CARSDATA1.csv'],  # Corrected source_objects
    destination_project_dataset_table="msdev-project-401207.msdevdataset1.carsdatatable",
    schema_fields=[{'name': 'price', 'type': 'INTEGER'}, {'name': 'engType', 'type': 'STRING'}, {'name': 'model', 'type': 'STRING'}],
    write_disposition='WRITE_TRUNCATE',
    source_format='CSV',
    autodetect=True,
    dag=dag,
)