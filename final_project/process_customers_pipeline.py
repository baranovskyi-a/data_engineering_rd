from datetime import datetime
from airflow import DAG
from airflow.models import Variable
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from process_customers_queries import CLEAR_BRONZE_TAB_QUERY, FILL_DUMP_DATE_QUERY, MERGE_TO_SILVER_QUERY

DEFAULT_ARGS = {
    'depends_on_past': True,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': 30,
}

GCS_BUCKET = Variable.get('GCS_BUCKET')
BRONZE_CUSTOMERS_TAB = Variable.get('BRONZE_CUSTOMERS_TAB')
CUSTOMERS_FOLDER_PATTERN = \
    'customers/'\
    + '-'.join([
        '{{ macros.ds_format(ds, "%Y-%m-%d", "%Y") }}',
        '{{ macros.ds_format(ds, "%Y-%m-%d", "%m") }}',
        '{{ macros.ds_format(ds, "%Y-%m-%d", "%-d") }}'
    ])\
    + '/*.csv'


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
        destination_project_dataset_table=BRONZE_CUSTOMERS_TAB,
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
