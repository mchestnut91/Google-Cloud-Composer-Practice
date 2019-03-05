import datetime as dt
from dependencies import text_message
from dependencies.text_message import textmyself

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.bigquery_to_gcs import BigQueryToCloudStorageOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator

from google.cloud import storage

#Establishing input bucket, input file name
input_bucket_id = 'insert bucket name'
input_file_name = 'insert file name'

#Establishing bigquery dataset, cloud storage bucket, output file for query results
bq_dataset_name = 'insert bigquery dataset name'
bq_table_id = bq_dataset_name + 'table name extension'
output_bucket_id = 'destination bucket'
output_file = 'gs://{gcs_bucket}/file name'.format(
    gcs_bucket=output_bucket_id)

#default arguments for airflow
default_args = {
    'owner': 'airflow',
    'start_date': dt.datetime(2019, 2, 21, 9, 00, 00),
    'concurrency': 1,
    'retries': 0
}

#Establishing DAG  
with DAG('example_dag',
         catchup=False,
         default_args=default_args,
         #schedule_interval='*/10 * * * *',
         schedule_interval=None,
         ) as dag:

    #Load csv into Bigquery
    load_data_to_bq = GoogleCloudStorageToBigQueryOperator(
        task_id='load_data_to_bq',
        bucket=input_bucket_id,
        source_objects=[input_file_name],
        destination_project_dataset_table=bq_dataset_name + '.destination table',
        source_format='CSV',
        schema_object='schema.json',
        skip_leading_rows=1,
        create_disposition='CREATE_IF_NEEDED',
        write_disposition='WRITE_TRUNCATE',
        google_cloud_storage_conn_id='google_cloud_storage_default',
        bigquery_conn_id='bigquery_default',
        field_delimiter=';')

    # Perform query of wine data
    bq_query = BigQueryOperator(
        task_id='bq_query',
        bql="""     SELECT * FROM `project.dataset.table` 
        WHERE quality >= 6.0 
        LIMIT 1000
        """,
        use_legacy_sql=False,
        destination_dataset_table=bq_table_id)

    #Export query result to Cloud Storage
    export_results_to_gcs = BigQueryToCloudStorageOperator(      
        task_id='export_results_to_gcs',
        source_project_dataset_table=bq_table_id,
        destination_cloud_storage_uris=[output_file],
        export_format='CSV')

    #Send text to notify me that the DAG has run
    text_notification = PythonOperator(
        task_id='text_notification',
        python_callable=textmyself,
        op_args=[text_message.message])

#Establishing order of tasks
load_data_to_bq >> bq_query >> export_results_to_gcs >> text_notification
