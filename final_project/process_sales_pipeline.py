from datetime import datetime
from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from process_sales_queries import DELETE_QUERY, INSERT_QUERY

DEFAULT_ARGS = {
    'depends_on_past': True,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': 30,
}

with DAG(
        dag_id='process_sales_pipeline',
        start_date=datetime(2022, 9, 1),
        end_date=datetime(2022, 10, 1),
        schedule_interval="0 1 * * *",
        catchup=True,
        max_active_runs=1,
        default_args=DEFAULT_ARGS,
) as dag:
    clear_silver_tab = BigQueryExecuteQueryOperator(
        task_id="clear_silver_tab",
        sql=DELETE_QUERY,
        use_legacy_sql=False,
        gcp_conn_id='gcp_conn'
    )
    insert_from_bronze_to_silver = BigQueryExecuteQueryOperator(
        task_id="insert_from_bronze_to_silver",
        sql=INSERT_QUERY,
        use_legacy_sql=False,
        gcp_conn_id='gcp_conn'
    )
    clear_silver_tab >> insert_from_bronze_to_silver
