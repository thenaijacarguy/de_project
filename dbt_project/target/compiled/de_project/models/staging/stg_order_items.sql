WITH source AS (
    SELECT * FROM raw.order_items
),

cleaned AS (
    SELECT
        TRIM(item_id)                          AS item_id,
        TRIM(order_id)                         AS order_id,
        TRIM(LOWER(product_id))                AS product_id,
        CAST(quantity AS INTEGER)              AS quantity,
        CAST(unit_price AS NUMERIC(10,2))      AS unit_price,

        -- Derived column: revenue for this line item.
        -- We calculate it here once so every downstream model can just
        -- use 'line_revenue' without repeating the formula everywhere.
        CAST(quantity AS INTEGER)
            * CAST(unit_price AS NUMERIC(10,2)) AS line_revenue,

        NOW()                                  AS dbt_loaded_at

    FROM source

    -- A quantity of 0 or less makes no business sense.
    -- It usually means a data entry error or a refund that was
    -- recorded incorrectly. We exclude these rows.
    WHERE CAST(quantity AS INTEGER) > 0
      AND TRIM(item_id) IS NOT NULL
)

SELECT * FROM cleaned