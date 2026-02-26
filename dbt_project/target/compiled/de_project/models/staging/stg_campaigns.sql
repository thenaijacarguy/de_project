-- The campaigns raw table has one row per date, with the entire
-- API response stored as JSON text. Here we unpack that JSON into
-- proper relational columns — one row per campaign per day.

WITH source AS (
    SELECT * FROM raw.campaigns
),

-- jsonb_array_elements() is a Postgres function that takes a JSON
-- array and returns one row per element. This is called "unnesting"
-- or "exploding" an array — we're turning [a, b, c] into three rows.
-- We cast raw_response to JSONB (binary JSON) so Postgres can parse it.
unnested AS (
    SELECT
        fetched_date,
        -- elem is one element from the JSON array — one campaign object
        elem AS campaign
    FROM source,
    -- The comma here is a LATERAL JOIN — it means "for each row in source,
    -- run this function and join its output back". This is how you unnest
    -- arrays in SQL.
    jsonb_array_elements(raw_response::jsonb) AS elem
),

cleaned AS (
    SELECT
        fetched_date::DATE                                AS report_date,

        -- The -> operator extracts a key from a JSON object as JSON.
        -- The ->> operator extracts it as plain TEXT.
        -- We use ->> when we want the value as a string to cast or use directly.
        (campaign ->> 'id')::INTEGER                     AS campaign_id,
        campaign ->> 'title'                             AS campaign_name,
        campaign ->> 'body'                              AS campaign_body,
        -- Note: our mock API (JSONPlaceholder) returns 'userId' not spend/clicks.
        -- In a real pipeline these would be spend_usd, clicks, impressions etc.
        (campaign ->> 'userId')::INTEGER                 AS user_id,

        NOW()                                            AS dbt_loaded_at

    FROM unnested
)

SELECT * FROM cleaned