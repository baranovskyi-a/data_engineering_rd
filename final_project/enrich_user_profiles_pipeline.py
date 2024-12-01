from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from enrich_user_profiles_queries import MERGE_TO_GOLD_QUERY

DEFAULT_ARGS = {
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': 30,
}

with DAG(
        dag_id='enrich_user_profiles_pipeline',
        max_active_runs=1,
        default_args=DEFAULT_ARGS,
) as dag:
    merge_to_gold = BigQueryExecuteQueryOperator(
        task_id="merge_to_gold",
        sql=MERGE_TO_GOLD_QUERY,
        use_legacy_sql=False,
        gcp_conn_id='gcp_conn'
    )
    merge_to_gold
