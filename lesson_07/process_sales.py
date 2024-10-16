import os
from datetime import datetime
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable

EXTRACT_JOB_HOST = 'localhost'
EXTRACT_JOB_PORT = 8081
CONVERT_JOB_HOST = 'localhost'
CONVERT_JOB_PORT = 8082
BASE_DIR = Variable.get('SALES_BASE_DIR')
REQUEST_TIMEOUT_SECONDS = 10
DEFAULT_ARGS = {
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': 30,
    'max_active_runs': 1,
}

def run_extract_job(**kwargs):
    resp = requests.post(
        url = f'http://{EXTRACT_JOB_HOST}:{EXTRACT_JOB_PORT}/',
        json={
            "date": kwargs['ds'],
            "raw_dir": os.path.join(BASE_DIR, "raw", "sales", kwargs['ds']),
        },
        timeout=REQUEST_TIMEOUT_SECONDS
    )
    assert resp.status_code == 201


def run_convert_job(**kwargs):
    resp = requests.post(
        url = f'http://{CONVERT_JOB_HOST}:{CONVERT_JOB_PORT}/',
        json={
            "raw_dir": os.path.join(BASE_DIR, "raw", "sales", kwargs['ds']),
            "stg_dir": os.path.join(BASE_DIR, "stg", "sales", kwargs['ds']),
        },
        timeout=REQUEST_TIMEOUT_SECONDS
    )
    assert resp.status_code == 201


with DAG(
        dag_id='process_sales',
        start_date=datetime(2022, 8, 9),
        end_date=datetime(2022, 8, 12),
        schedule_interval="0 1 * * *",
        catchup=True,
        default_args=DEFAULT_ARGS,
) as dag:
    extract = PythonOperator(
        task_id='extract_data_from_api',
        python_callable=run_extract_job,
        provide_context=True,
    )

    convert = PythonOperator(
        task_id='convert_to_avro',
        python_callable=run_convert_job,
        provide_context=True,
    )

    extract >> convert
