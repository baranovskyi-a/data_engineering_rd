SELECT 
  U.state
FROM `de-07-andrii-baranovskyi-2.fnl_prjct_silver.sales` S
LEFT JOIN `de-07-andrii-baranovskyi-2.fnl_prjct_gold.user_profiles_enriched` U
ON S.client_id = U.client_id
WHERE 
  S.product_name = 'TV'
  AND DATE_DIFF(S.purchase_date, U.birth_date, YEAR) BETWEEN 20 AND 30
  AND S.purchase_date BETWEEN DATE('2022-09-01') AND DATE('2022-09-10')
GROUP BY U.state
ORDER BY COUNT(S.client_id) DESC
LIMIT 1

-- Idaho