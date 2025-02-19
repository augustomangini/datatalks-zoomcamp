from airflow import DAG
import os
from datetime import timedelta
from airflow.utils.dates import datetime
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from ingest_script import ingest_callable

default_args={
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    "start_date": datetime(2019, 1, 1),
    "end_date": datetime(2019, 1, 1),
    "schedule_interval": "0 6 2 * *",
}

os.getenv("PG_HOST")
os.getenv("PG_USER")
os.getenv("PG_PASSWORD")
os.getenv("PG_PORT")
os.getenv("PG_DATABASE")

path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow")
#dataset_file = "yellow_tripdata_2021-02.csv"
color_taxi = "yellow/"
table_name_color = "yellow_taxi_"
#parquet_file = dataset_file.replace('.csv', '.parquet')
#dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color_taxi}/{dataset_file}.gz"
#zipped_file = f"{dataset_file}.gz" 
url_prefix = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/"
url_template = url_prefix + color_taxi + "/yellow_tripdata_{{ execution_date.strftime(\"%Y-%m\") }}.csv.gz"
output_file_template = path_to_local_home + "/output_{{ execution_date.strftime(\"%Y-%m\") }}.csv.gz"
table_name_template = f"{table_name_color}" + "{{ execution_date.strftime(\"%Y_%m\") }}"


local_workflow = DAG(
    "data_ingestion_localDB",
    description="DAG to ingest data to local database",
    default_args=default_args,
    tags=["data-ingestion", "localDB"],
)

# Define the tasks
with local_workflow:
    
    wget_task = BashOperator(
        task_id="wget",
        bash_command=f"wget -O {output_file_template} {url_template}",
        #bash_command="echo '{{ execution_date.strftime(\"%Y-%m\") }}'",
    )
    
    ingest_task = PythonOperator(
        task_id="ingest",
        python_callable=ingest_callable,
        op_kwargs={
            "user": os.getenv("PG_USER"),
            "password": os.getenv("PG_PASSWORD"),
            "host": os.getenv("PG_HOST"),
            "port": os.getenv("PG_PORT"),
            "db": os.getenv("PG_DATABASE"),
            "table_name": table_name_template,
            "csv_file": output_file_template,
        },
    )
    
    wget_task >> ingest_task