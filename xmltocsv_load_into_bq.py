from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import csv
from google.cloud import storage
from google.cloud import bigquery
from datetime import datetime

# Define your GCS bucket and CSV file information
gcs_bucket_name = "your-gcs-bucket"
csv_file_name = "output.csv"

def transform_bq_to_csv():
    # Initialize Google Cloud Storage client
    storage_client = storage.Client()

    # Extract data from BigQuery and transform to CSV
    bq_client = bigquery.Client()
    query = """
    SELECT customer_id, first_name, last_name, email
    FROM your_project.your_dataset.your_table
    """
    query_job = bq_client.query(query)

    # Define the CSV data
    csv_data = []

    for row in query_job:
        data = [str(row.customer_id), row.first_name, row.last_name, row.email]
        csv_data.append(data)

    # Write the CSV data to a CSV file
    with open(csv_file_name, mode='w', newline='') as csv_file:
        writer = csv.writer(csv_file)
        writer.writerows(csv_data)

def upload_csv_to_gcs():
    # Upload the CSV file to GCS
    storage_client = storage.Client()
    bucket = storage_client.bucket(gcs_bucket_name)
    blob = bucket.blob(csv_file_name)
    blob.upload_from_filename(csv_file_name)

    print(f"CSV file '{csv_file_name}' uploaded to GCS bucket '{gcs_bucket_name}'")

# Define your Airflow DAG
default_args = {
    'owner': "Mahesh",
    'start_date': datetime(2023, 10, 25),
    'retries': 1,
}

dag = DAG(
    'bq_to_csv_and_upload_to_gcs',
    schedule_interval='@daily',
    catchup=False,
    default_args=default_args,
    description="Transform BigQuery data to CSV and upload to GCS",
)

# Define PythonOperators to execute the tasks
transform_task = PythonOperator(
    task_id='transform_bq_to_csv',
    python_callable=transform_bq_to_csv,
    dag=dag,
)

upload_task = PythonOperator(
    task_id='upload_csv_to_gcs',
    python_callable=upload_csv_to_gcs,
    dag=dag,
)

# Set task dependencies
transform_task >> upload_task
