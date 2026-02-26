-- This model cleans the raw customers table.
-- We reference raw.customers using the source() function (configured later)
-- but for now we'll reference it directly.

WITH source AS (
    -- Pull everything from the raw table first.
    -- We always start with a CTE called 'source' — it makes it clear
    -- exactly where the data is coming from.
    SELECT * FROM raw.customers
),

cleaned AS (
    SELECT
        -- TRIM() removes leading and trailing whitespace.
        -- LOWER() standardises to lowercase.
        -- We do both because source data is often inconsistent —
        -- "  CUST0001 " and "cust0001" should be treated as the same customer.
        TRIM(LOWER(customer_id))           AS customer_id,

        -- INITCAP() capitalises the first letter of each word.
        -- So "john smith" becomes "John Smith". Consistent name formatting
        -- makes reports look professional and avoids duplicate-looking records.
        INITCAP(TRIM(name))                AS customer_name,

        -- Store email in lowercase — emails are case-insensitive and
        -- "John@Email.com" vs "john@email.com" are the same address.
        TRIM(LOWER(email))                 AS email,

        -- CAST converts the data type. Remember we stored everything
        -- as TEXT in raw. Here we convert signup_date to an actual DATE
        -- type so we can do date arithmetic on it later (e.g. days since signup).
        CAST(signup_date AS DATE)          AS signup_date,

        TRIM(region)                       AS region,

        -- We add a metadata column recording when THIS model ran.
        -- NOW() returns the current timestamp.
        -- This is useful for debugging — you can see exactly when
        -- each row was last processed.
        NOW()                              AS dbt_loaded_at

    FROM source

    -- Filter out any rows where customer_id is null or empty.
    -- A customer with no ID is useless — we can't link them to orders.
    WHERE TRIM(customer_id) IS NOT NULL
      AND TRIM(customer_id) != ''
)

SELECT * FROM cleaned