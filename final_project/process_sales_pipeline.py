from datetime import datetime
from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator

DEFAULT_ARGS = {
    'depends_on_past': True,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': 30,
}

DELETE_QUERY = '''
DELETE 
FROM `de-07-andrii-baranovskyi-2.fnl_prjct_silver.sales` 
WHERE purchase_date = DATE('{{ ds }}')
'''

INSERT_QUERY = '''
INSERT INTO `de-07-andrii-baranovskyi-2.fnl_prjct_silver.sales` 
WITH tab2insert AS (
  SELECT 
    CAST(CustomerId AS int64) client_id,
    CASE 
      WHEN REGEXP_CONTAINS(PurchaseDate, r'^\d{4}-\d{2}-\d{1,2}$') THEN DATE(PurchaseDate)
      WHEN REGEXP_CONTAINS(PurchaseDate, r'^\d{4}/\d{2}/\d{1,2}$') THEN PARSE_DATE('%Y/%m/%e', PurchaseDate)
      WHEN REGEXP_CONTAINS(PurchaseDate, r'^\d{4}-[a-zA-Z]{3}-\d{1,2}$') THEN PARSE_DATE('%Y-%b-%e', PurchaseDate)
      ELSE DATE(PurchaseDate)
    END purchase_date,
    Product product_name, 
    CAST(REPLACE(REPLACE(Price, '$', ''), 'USD', '') AS float64) price
  FROM `de-07-andrii-baranovskyi-2.fnl_prjct_bronze.sales` 
)
SELECT * FROM tab2insert
WHERE purchase_date = DATE('{{ ds }}')
'''

with DAG(
        dag_id='process_sales_pipeline',
        start_date=datetime(2022, 9, 1),
        end_date=datetime(2022, 10, 2),
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
