from airflow import DAG 
from datetime import datetime 
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator 

default_args= {
    'owner':"Mahesh",
    'start_date': datetime(2023,10,27),
    'retries':1,

}

dag=DAG(
    'mysql_data_trnfr_gcs_to_bq',
    schedule_interval = "@daily",
    catchup=False,
    default_args=default_args,
    description="file tranfer from gcs to bq",
)

gcs_bq_task=GCSToBigQueryOperator(
    task_id='data_transfer_from_gcs_to_bq',
    bucket='dagbkt',
    source_objects=['account_info.csv'],
    destination_project_dataset_table="msdev-project-401207.msdevdataset1.sqlgcsdata",
    schema_fields=[{'name':'id', 'type':'INTEGER'}, {'name':'name', 'type':'STRING'},
                   {'name':'phonenumber', 'type':'INTEGER'}, {'name':'emailid', 'type':'STRING'}],
    write_disposition='WRITE_TRUNCATE',
    source_format='CSV',
    autodetect=True,
    dag=dag,
)

gcs_bq_task