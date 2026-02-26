WITH source AS (
    SELECT * FROM raw.orders
),

cleaned AS (
    SELECT
        TRIM(order_id)                         AS order_id,
        TRIM(LOWER(customer_id))               AS customer_id,
        CAST(order_date AS DATE)               AS order_date,

        -- Normalise status to lowercase and remove any test/internal orders.
        -- We filter those out in the WHERE clause below.
        TRIM(LOWER(status))                    AS status,

        CAST(total_amount AS NUMERIC(10,2))    AS total_amount,
        NOW()                                  AS dbt_loaded_at

    FROM source

    WHERE TRIM(order_id) IS NOT NULL
      AND TRIM(order_id) != ''
      -- Filter out cancelled test orders. In the source system, developers
      -- sometimes place test orders with status 'test' or 'internal'.
      -- These should never appear in analytics.
      AND TRIM(LOWER(status)) NOT IN ('test', 'internal')
)

SELECT * FROM cleaned