-- This is the most complex staging model because the logistics CSV
-- is the messiest source. Multiple date formats, "kg" suffixes,
-- inconsistent carrier names — we fix all of it here.

WITH source AS (
    SELECT * FROM raw.shipments
),

cleaned AS (
    SELECT
        TRIM(shipment_id)      AS shipment_id,

        -- The order_reference in the CSV doesn't match order_id in the
        -- orders table directly. The logistics provider includes a prefix
        -- like "REF-ORD00001" but our orders table has "ORD00001".
        -- We use REPLACE() to strip the prefix so we can join later.
        REPLACE(TRIM(order_reference), 'REF-', '') AS order_id,

        -- The CSV has two date formats mixed together:
        --   MM/DD/YYYY  e.g. "01/15/2024"
        --   YYYY-MM-DD  e.g. "2024-01-15"
        -- We use a CASE statement to detect which format each row uses
        -- and parse it correctly.
        CASE
            -- If the 3rd character is '/' it's MM/DD/YYYY format
            WHEN SUBSTRING(dispatch_date, 3, 1) = '/'
            THEN TO_DATE(dispatch_date, 'MM/DD/YYYY')
            -- Otherwise assume YYYY-MM-DD (ISO format)
            ELSE CAST(dispatch_date AS DATE)
        END                    AS dispatch_date,

        -- delivery_date can be blank (shipment not yet delivered).
        -- We use NULLIF() to convert empty strings to proper SQL NULLs.
        -- NULLIF(a, b) returns NULL if a equals b, otherwise returns a.
        -- Without this, you'd have empty string "" instead of NULL,
        -- which breaks date functions and looks wrong in reports.
        CASE
            WHEN NULLIF(TRIM(delivery_date), '') IS NULL THEN NULL
            WHEN SUBSTRING(delivery_date, 3, 1) = '/'
            THEN TO_DATE(delivery_date, 'MM/DD/YYYY')
            ELSE CAST(delivery_date AS DATE)
        END                    AS delivery_date,

        -- Standardise carrier names: uppercase and trim.
        -- "fedex", "Fedex", "FEDEX", " FedEx " all become "FEDEX"
        UPPER(TRIM(carrier))   AS carrier,

        TRIM(UPPER(status))    AS status,

        -- The weight column has messy values: "2.5kg", "3.0 kg", "1.2".
        -- REGEXP_REPLACE() uses a regular expression to remove any
        -- non-numeric characters (except the decimal point).
        -- '[^0-9.]' means "anything that is NOT a digit or a dot".
        CAST(
            REGEXP_REPLACE(weight_kg, '[^0-9.]', '', 'g')
            AS NUMERIC(10,2)
        )                      AS weight_kg,

        source_file,
        NOW()                  AS dbt_loaded_at

    FROM source

    WHERE TRIM(shipment_id) IS NOT NULL
)

SELECT * FROM cleaned