-- The date dimension is special — it's not derived from any source table.
-- We generate it from scratch using SQL. It gives us one row per calendar
-- day for the years we care about (2023–2025 covers our dataset).
--
-- Why bother? Because analysts always want to group by time: by month,
-- by quarter, by day of week. Storing those attributes in a dimension
-- means they don't have to recalculate them every time in every query.

WITH date_spine AS (
    -- generate_series() is a Postgres function that generates a sequence.
    -- Here it generates one timestamp per day from 2023-01-01 to 2025-12-31.
    SELECT
        GENERATE_SERIES(
            '2023-01-01'::TIMESTAMP,
            '2025-12-31'::TIMESTAMP,
            '1 day'::INTERVAL
        ) AS date_day
)

SELECT
    date_day::DATE                          AS date_id,      -- the primary key
    date_day::DATE                          AS full_date,
    EXTRACT(YEAR  FROM date_day)::INTEGER   AS year,
    EXTRACT(MONTH FROM date_day)::INTEGER   AS month_number,
    TO_CHAR(date_day, 'Month')              AS month_name,
    EXTRACT(QUARTER FROM date_day)::INTEGER AS quarter,
    EXTRACT(DOW FROM date_day)::INTEGER     AS day_of_week,  -- 0=Sunday, 6=Saturday
    TO_CHAR(date_day, 'Day')                AS day_name,
    CASE
        WHEN EXTRACT(DOW FROM date_day) IN (0, 6) THEN TRUE
        ELSE FALSE
    END                                     AS is_weekend

FROM date_spine