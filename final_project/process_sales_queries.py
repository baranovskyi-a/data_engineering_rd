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
