from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator

DEFAULT_ARGS = {
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': 30,
}

MERGE_TO_GOLD_QUERY = '''
MERGE INTO `de-07-andrii-baranovskyi-2.fnl_prjct_gold.user_profiles_enriched` T
USING (
  SELECT 
    C.client_id, 
    UP.full_name, 
    C.email, 
    C.registration_date, 
    UP.state, 
    UP.birth_date, 
    UP.phone_number
  FROM `de-07-andrii-baranovskyi-2.fnl_prjct_silver.customers` C
  LEFT JOIN `de-07-andrii-baranovskyi-2.fnl_prjct_silver.user_profiles` UP
  ON C.email = UP.email
) S
ON T.client_id = S.client_id
WHEN MATCHED THEN
  UPDATE SET 
    full_name = S.full_name,
    email = S.email,
    registration_date = S.registration_date,
    state = S.state,
    birth_date = S.birth_date,
    phone_number = S.phone_number
WHEN NOT MATCHED BY TARGET THEN
  INSERT (client_id, full_name, email, registration_date, state, birth_date, phone_number)
  VALUES(S.client_id, S.full_name, S.email, S.registration_date, S.state, S.birth_date, S.phone_number)
'''

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
