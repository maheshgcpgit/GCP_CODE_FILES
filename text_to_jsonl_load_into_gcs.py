from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import json
from google.cloud import storage

default_args = {
    'owner': 'your-name',
    'start_date': datetime(2023, 10, 25),
    'retries': 1,
}

dag = DAG(
    'text_to_jsonl_conversion',
    default_args=default_args,
    schedule_interval=None,  # Set your desired schedule interval
)

source_bucket_name = 'ms-bkt1'
source_blob_name = 'TASKDATA.txt'
target_bucket_name = 'ms-bkt2'
target_blob_name = 'jsonl_data_file.jsonl'

# Define a function to convert text to JSONL
def text_to_jsonl(source_text):
    jsonl_data = []
    for line in source_text.split('\n'):
        if line.strip():
            data = line.strip()
            jsonl_data.append(json.dumps(data))

    return "\n".join(jsonl_data)

# Task to download the source text file from GCS and perform the conversion
def convert_and_upload_to_gcs():
    storage_client = storage.Client()
    source_bucket = storage_client.bucket(source_bucket_name)
    source_blob = source_bucket.blob(source_blob_name)
    
    # Download source text from GCS
    source_text = source_blob.download_as_text()
    
    # Perform the text-to-JSONL conversion
    jsonl_data = text_to_jsonl(source_text)

    # Upload JSONL data to the target GCS bucket
    target_bucket = storage_client.bucket(target_bucket_name)
    target_blob = target_bucket.blob(target_blob_name)
    target_blob.upload_from_string(jsonl_data, content_type='application/jsonl')

convert_task = PythonOperator(
    task_id='convert_and_upload_to_gcs',
    python_callable=convert_and_upload_to_gcs,
    provide_context=True,
    dag=dag,
)