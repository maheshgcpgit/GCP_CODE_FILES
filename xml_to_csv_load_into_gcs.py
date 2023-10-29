from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import xml.etree.ElementTree as ET
import csv
from google.cloud import storage
from datetime import datetime

# Define your GCS bucket, XML file, and CSV file information
gcs_bucket_name = "dagbkt"
xml_file_path = "xmldatafile.xml"  # Path to your XML file in GCS
csv_file_name = "result.csv"

def transform_xml_to_csv():
    # Initialize Google Cloud Storage client
    storage_client = storage.Client()

    # Read the XML file from GCS
    bucket = storage_client.bucket(gcs_bucket_name)
    blob = bucket.blob(xml_file_path)
    xml_content = blob.download_as_text()

    # Parse the XML content
    root = ET.fromstring(xml_content)

    # Define the CSV data
    csv_data = []

    # Extract data from the XML and add it to the CSV data
    for customer in root.findall('customer'):
        data = []
        data.append(customer.find('customer_id').text)
        data.append(customer.find('first_name').text)
        data.append(customer.find('last_name').text)
        data.append(customer.find('email').text)

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
    'xml_to_csv_and_upload_to_gcs',
    schedule_interval='@daily',
    catchup=False,
    default_args=default_args,
    description="Transform XML data from GCS to CSV and upload back to GCS",
)

# Define PythonOperators to execute the tasks
transform_task = PythonOperator(
    task_id='transform_xml_to_csv',
    python_callable=transform_xml_to_csv,
    dag=dag,
)

upload_task = PythonOperator(
    task_id='upload_csv_to_gcs',
    python_callable=upload_csv_to_gcs,
    dag=dag,
)

# Set task dependencies
transform_task >> upload_task