-- The customer dimension answers: "who bought this?"
-- Every row in fact_sales will link back to one row here via customer_id.

WITH customers AS (
    -- We reference the staging model, not the raw table.
    -- In dbt, ref() is how you reference another model.
    -- This tells dbt about the dependency — it will run stg_customers
    -- before it runs dim_customers. Never skip the staging layer.
    SELECT * FROM "warehouse"."staging"."stg_customers"
)

SELECT
    -- In a dimension table, the primary key gets renamed to a
    -- 'surrogate key' style name for clarity in queries.
    customer_id,
    customer_name,
    email,
    signup_date,
    region,

    -- How many days has this customer been with us?
    -- CURRENT_DATE - signup_date gives an INTERVAL in Postgres,
    -- EXTRACT(DAY FROM ...) pulls the number of days from that interval.
    (CURRENT_DATE - signup_date::DATE) AS days_since_signup,

    dbt_loaded_at

FROM customers