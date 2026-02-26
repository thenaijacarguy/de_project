
  
    

  create  table "warehouse"."marts"."dim_products__dbt_tmp"
  
  
    as
  
  (
    -- The product dimension answers: "what was bought?"

WITH products AS (
    SELECT * FROM "warehouse"."staging"."stg_products"
)

SELECT
    product_id,
    product_name,
    category,
    cost_price,
    dbt_loaded_at

FROM products
  );
  