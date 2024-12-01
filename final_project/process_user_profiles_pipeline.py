from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from process_user_profiles_queries import MERGE_TO_SILVER_QUERY

DEFAULT_ARGS = {
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': 30,
}

with DAG(
        dag_id='process_user_profiles_pipeline',
        max_active_runs=1,
        default_args=DEFAULT_ARGS,
) as dag:
    merge_to_silver = BigQueryExecuteQueryOperator(
        task_id="merge_to_silver",
        sql=MERGE_TO_SILVER_QUERY,
        use_legacy_sql=False,
        gcp_conn_id='gcp_conn'
    )
    merge_to_silver
