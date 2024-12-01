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
