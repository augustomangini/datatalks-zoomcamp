import os
from airflow import DAG
from datetime import timedelta
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator
import gzip
import shutil
import pyarrow.csv as pv
import pyarrow.parquet as pq
from google.cloud import storage

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
BIGQUERY_DATASET = os.environ.get("GCP_BIGQUERY_DATASET", "sa_dataset")
path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow")
#dataset_file = "green_tripdata_2020-12.csv"
color_taxi = "yellow"
#parquet_file = dataset_file.replace('.csv', '.parquet')
#dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color_taxi}/{dataset_file}.gz"

default_args={
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    "start_date": days_ago(1),
    "schedule_interval": "@daily",
}

with DAG(
    "data_ingestion_GCS",
    schedule_interval="@daily",
    description="DAG to ingest data to GCS",
    default_args=default_args,
    tags=["data-ingestion", "GCS"],
) as dag:
    
    for year in range(2019, 2021):
        for month in range(1, 13):
            dataset_file = f"{color_taxi}_tripdata_{year}-{month:02d}.csv"
            
            parquet_file = dataset_file.replace('.csv', '.parquet')
            dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color_taxi}/{dataset_file}.gz"
        
            #funçoes de utilidade para o DAG:
            def unzilp_file(file_gz, output_file):
                if not os.path.exists(file_gz):
                    raise FileNotFoundError(f"GZ file not found: {file_gz}")
                try:
                    with gzip.open(file_gz, "rb") as f_in:
                        with open(output_file, "wb") as f_out:
                            shutil.copyfileobj(f_in, f_out)

                    if not os.path.exists(output_file):
                        raise RuntimeError(f"Extraction failed: {output_file} not created")

                    print(f"Successfully extracted: {output_file}")

                except Exception as e:
                    raise RuntimeError(f"Error extracting {file_gz}: {e}")

            def format_to_parquet(src_file):
                table = pv.read_csv(src_file)
                pq.write_table(table, src_file.replace('.csv', '.parquet'))

            #Upload do arquivo para o GCS:
            def upload_to_gcs(bucket, object_name, local_file):
                """
                Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
                :param bucket: GCS bucket name
                :param object_name: target path & file-name
                :param local_file: source path & file-name
                :return:
                """
                # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
                # (Ref: https://github.com/googleapis/python-storage/issues/74)
                storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
                storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB
                # End of Workaround

                client = storage.Client()
                bucket = client.bucket(bucket)
                blob = bucket.blob(object_name)
                blob.upload_from_filename(local_file)

            #########################################################################################################################

            download_dataset_file = BashOperator(
                task_id=f"download_dataset_file_{year}_{month:02d}",
                bash_command=f'wget -O {path_to_local_home}/{dataset_file}.gz {dataset_url}',
                do_xcom_push=True
            )

            unzip_dataset_file = PythonOperator(
                task_id=f"unzip_dataset_file_{year}_{month:02d}",
                python_callable=unzilp_file,
                op_kwargs={
                    "file_gz": f"{path_to_local_home}/{dataset_file}.gz",
                    "output_file": f"{path_to_local_home}/{dataset_file}"
                },
                do_xcom_push=True
            )

            convert_to_parquet = PythonOperator(
                task_id=f"convert_to_parquet_{year}_{month:02d}",
                python_callable=format_to_parquet,
                op_kwargs={
                    "src_file": f"{path_to_local_home}/{dataset_file}"
                },
                do_xcom_push=True
            )

            local_to_gcs = PythonOperator(
                task_id=f"local_to_gcs_{year}_{month:02d}",
                python_callable=upload_to_gcs,
                op_kwargs={
                    "bucket": BUCKET,
                    "object_name": f"raw/{parquet_file}",
                    "local_file": f"{path_to_local_home}/{parquet_file}"
                },
                do_xcom_push=True
            )

            bigquery_external_table_task = BigQueryCreateExternalTableOperator(
                task_id=f"bigquery_external_table_task_{year}_{month:02d}",
                table_resource={
                    "tableReference": {
                        "projectId": PROJECT_ID,
                        "datasetId": BIGQUERY_DATASET,
                        "tableId": f"{color_taxi}_taxi_{year}_{month:02d}",
                    },
                    "externalDataConfiguration": {
                        "sourceFormat": "PARQUET",
                        "sourceUris": [f"gs://{BUCKET}/raw/{parquet_file}"],
                    },
                },
                do_xcom_push=True
            )

            download_dataset_file >> unzip_dataset_file >> convert_to_parquet >> local_to_gcs >> bigquery_external_table_task