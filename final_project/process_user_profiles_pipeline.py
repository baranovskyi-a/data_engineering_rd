from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator

DEFAULT_ARGS = {
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': 30,
}

MERGE_TO_SILVER_QUERY = '''
MERGE INTO `de-07-andrii-baranovskyi-2.fnl_prjct_silver.user_profiles` T
USING (SELECT 
  email,
  full_name,
  state,
  DATE(birth_date) birth_date,
  REGEXP_REPLACE(phone_number, r'[().-]', '') AS phone_number
FROM 
  `de-07-andrii-baranovskyi-2.fnl_prjct_bronze.user_profiles`) S
ON T.email = S.email
WHEN MATCHED THEN
  UPDATE SET 
    full_name = S.full_name,
    state = S.state,
    birth_date = S.birth_date,
    phone_number = S.phone_number
WHEN NOT MATCHED BY TARGET THEN
  INSERT (email, full_name, state, birth_date, phone_number)
  VALUES(S.email, S.full_name, S.state, S.birth_date, S.phone_number)
WHEN NOT MATCHED BY SOURCE THEN
  DELETE
'''

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
