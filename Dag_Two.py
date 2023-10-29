from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import xml.etree.ElementTree as ET
import json
from google.cloud import storage
from google.cloud.bigquery import SchemaField
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.models import Variable
from airflow.models import XCom

def xml_to_json_from_gcs(**context):
    bucket_name = 'dagbkt'
    file_name = 'xmldatafile.xml'

    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(file_name)

    # Download the XML file from GCS
    xml_data = blob.download_as_text()

    # Your XML to JSON conversion logic here
    root = ET.fromstring(xml_data)
    json_list = []

    for customer_elem in root.findall('customer'):
        customer = {
            'customer_id': customer_elem.find('customer_id').text,
            'first_name': customer_elem.find('first_name').text,
            'last_name': customer_elem.find('last_name').text,
            'email': customer_elem.find('email').text
        }
        
        # Check if any of the fields are empty
        if None in customer.values():
            raise Exception("XML data has missing fields")

        json_list.append(json.dumps(customer))

    # Define the BigQuery schema for the destination table
    schema = [
        SchemaField("customer_id", "STRING"),
        SchemaField("first_name", "STRING"),
        SchemaField("last_name", "STRING"),
        SchemaField("email", "STRING"),
    ]

    # Construct newline-delimited JSON by joining the JSON objects
    newline_delimited_json = '\n'.join(json_list)

    Variable.set("schema", json.dumps([field.to_api_repr() for field in schema]))
    XCom.set(key='json_data', value=newline_delimited_json, execution_date=context['execution_date'])

default_args = {
    'owner': "MAHESH",
    'start_date': datetime(2023, 10, 20),
    'retries': 1,
}

dag = DAG(
    'xml_datafile_processing_dag',
    schedule_interval="@daily",
    catchup=False,
    default_args=default_args,
    description="This dag processes XML data every day at 8:22 PM",
)

xml_process_task = PythonOperator (
    task_id="process_xml",
    python_callable=xml_to_json_from_gcs,
    provide_context=True,
    dag=dag,
)

def create_load_to_bq_task(**context):
    schema = json.loads(Variable.get("schema"))
    json_data = XCom.get_one(task_ids='process_xml', key='json_data', execution_date=context['execution_date'])
    
    load_to_bq_task = GCSToBigQueryOperator(
        task_id="load_json_to_bq",
        bucket='us-central1-cmp-4b4ac71a-bucket',
        source_objects=['env_var.json'],
        destination_project_dataset_table="msdev-project-401207.msdevdataset1.datatable",
        source_format='NEWLINE_DELIMITED_JSON',
        write_disposition='WRITE_TRUNCATE',
        autodetect=False,
        schema=schema,
        skip_leading_rows=1,
        dag=context['dag'],
    )
    return load_to_bq_task

xml_process_task >> create_load_to_bq_task
