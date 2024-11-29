from datetime import datetime
from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator


DEFAULT_ARGS = {
    'depends_on_past': True,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': 30,
}

GCS_BUCKET = 'fnl_prjct_raw'
CUSTOMERS_FOLDER_PATTERN = \
    'customers/'\
    + '-'.join([
        '{{ macros.ds_format(ds, "%Y-%m-%d", "%Y") }}',
        '{{ macros.ds_format(ds, "%Y-%m-%d", "%m") }}',
        '{{ macros.ds_format(ds, "%Y-%m-%d", "%-d") }}'
    ])\
    + '/*.csv'

CLEAR_BRONZE_TAB_QUERY = '''
DELETE 
FROM `de-07-andrii-baranovskyi-2.fnl_prjct_bronze.customers` 
WHERE 
    dump_date = DATE('{{ ds }}')
    OR dump_date IS NULL
'''

FILL_DUMP_DATE_QUERY = '''
UPDATE `de-07-andrii-baranovskyi-2.fnl_prjct_bronze.customers` 
SET dump_date = DATE('{{ ds }}')
WHERE dump_date is NULL
'''

MERGE_TO_SILVER_QUERY = '''
MERGE INTO `de-07-andrii-baranovskyi-2.fnl_prjct_silver.customers` T
USING (
  SELECT 
  DISTINCT 
    CAST(Id AS int64) client_id,
    FirstName first_name,
    LastName last_name,
    Email email,
    DATE(RegistrationDate) registration_date,
    State state
  FROM `de-07-andrii-baranovskyi-2.fnl_prjct_bronze.customers` 
  WHERE dump_date = DATE('{{ ds }}')
) S
ON T.client_id = S.client_id
WHEN MATCHED THEN
  UPDATE SET 
    first_name = S.first_name,
    last_name = S.last_name,
    email = S.email,
    registration_date = S.registration_date,
    state = S.state
WHEN NOT MATCHED BY TARGET THEN
  INSERT (client_id, first_name, last_name, email, registration_date, state)
  VALUES(S.client_id, S.first_name, S.last_name, S.email, S.registration_date, S.state)
'''


with DAG(
        dag_id='process_customers_pipeline',
        start_date=datetime(2022, 8, 1),
        end_date=datetime(2022, 8, 6),
        schedule_interval="0 1 * * *",
        catchup=True,
        max_active_runs=1,
        default_args=DEFAULT_ARGS,
) as dag:
    clear_bronze_tab = BigQueryExecuteQueryOperator(
        task_id="clear_bronze_tab",
        sql=CLEAR_BRONZE_TAB_QUERY,
        use_legacy_sql=False,
        gcp_conn_id='gcp_conn'
    )
    load_to_bronze = GCSToBigQueryOperator(
        task_id='load_to_bronze',
        bucket=GCS_BUCKET,
        source_objects=CUSTOMERS_FOLDER_PATTERN,
        destination_project_dataset_table='de-07-andrii-baranovskyi-2.fnl_prjct_bronze.customers',
        schema_fields=[
            {'name': 'Id', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'FirstName', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'LastName', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'Email', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'RegistrationDate', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'State', 'type': 'STRING', 'mode': 'NULLABLE'},
        ],
        skip_leading_rows=1,
        write_disposition='WRITE_APPEND',
        source_format='CSV',
        field_delimiter=',',
        gcp_conn_id='gcp_conn'
    )
    fill_dump_date = BigQueryExecuteQueryOperator(
        task_id="fill_dump_date",
        sql=FILL_DUMP_DATE_QUERY,
        use_legacy_sql=False,
        gcp_conn_id='gcp_conn'
    )
    merge_to_silver = BigQueryExecuteQueryOperator(
        task_id="merge_to_silver",
        sql=MERGE_TO_SILVER_QUERY,
        use_legacy_sql=False,
        gcp_conn_id='gcp_conn'
    )

    clear_bronze_tab >> load_to_bronze >> fill_dump_date >> merge_to_silver
