
  create view "warehouse"."staging"."stg_products__dbt_tmp"
    
    
  as (
    WITH source AS (
    SELECT * FROM raw.products
),

cleaned AS (
    SELECT
        TRIM(LOWER(product_id))            AS product_id,
        TRIM(name)                         AS product_name,

        -- Normalise category: lowercase and trimmed.
        -- "Electronics", " electronics", "ELECTRONICS" all become "electronics"
        TRIM(LOWER(category))              AS category,

        -- cost_price is stored as TEXT in raw. We cast it to NUMERIC
        -- so we can do maths with it (calculate profit margins etc).
        -- NUMERIC(10,2) means up to 10 digits total, 2 after the decimal.
        CAST(cost_price AS NUMERIC(10,2))  AS cost_price,

        NOW()                              AS dbt_loaded_at

    FROM source

    WHERE TRIM(product_id) IS NOT NULL
      AND TRIM(product_id) != ''
)

SELECT * FROM cleaned
  );