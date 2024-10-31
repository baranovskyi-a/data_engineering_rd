import os
import shutil
import json
from datetime import datetime
import pandas as pd
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow import macros


BASE_DIR = Variable.get('SALES_BASE_DIR')
AUTH_TOKEN = Variable.get('secret_fake_api_token')
REQUEST_TIMEOUT_SECONDS = 10
DEFAULT_ARGS = {
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': 30,
    'max_active_runs': 1,
}


def run_extract_job(raw_dir: str, ds: str) -> None:
    '''
        Loads sales from https://fake-api-vycpfa6oca-uc.a.run.app/sales and 
            saves them to raw_dir in .csv format. All data in raw_dir will be rewrited
        Params:
            ds - date in "yyyy-mm-dd" format
            raw_dir - dir to save
    '''
    page = 1
    records = []
    while True:
        # request current page
        response = requests.get(
            url='https://fake-api-vycpfa6oca-uc.a.run.app/sales',
            params={'date': ds, 'page': page},
            headers={'Authorization': AUTH_TOKEN},
            timeout=REQUEST_TIMEOUT_SECONDS
        )
        # break if there are no more records for current date
        if response.status_code == 404:
            break
        # append current batch
        response_json = json.loads(response.text)
        records += response_json
        page += 1
    dir_to_save = os.path.join(raw_dir, ds)
    # ensure idempotency
    if os.path.isdir(dir_to_save):
        shutil.rmtree(dir_to_save)
    os.makedirs(dir_to_save)
    # save .csv
    pd.DataFrame(records).to_csv(os.path.join(dir_to_save, f'sales_{ds}.csv'), index=False)


with DAG(
        dag_id='process_sales_gcp',
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
        op_kwargs={'raw_dir': BASE_DIR}
    )
    load = LocalFilesystemToGCSOperator(
        task_id='load_to_gcp',
        src=os.path.join(BASE_DIR, "{{ ds }}", 'sales_{{ ds }}.csv'),
        dst='/' + '/'.join([
            '{{ macros.ds_format(ds, "%Y-%m-%d", "%Y") }}',
            '{{ macros.ds_format(ds, "%Y-%m-%d", "%d") }}',
            '{{ macros.ds_format(ds, "%Y-%m-%d", "%m") }}',
            'sales_{{ ds }}.csv'
        ]),
        bucket='rd-ab-bucket',
        gcp_conn_id='gcp_conn'
    )

    extract >> load
