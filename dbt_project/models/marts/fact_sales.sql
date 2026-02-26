-- The fact table is the centrepiece of the warehouse.
-- Grain: one row per order item (the most granular level of a sale).
-- Every numeric measure an analyst might want is here.
-- Every foreign key links out to a dimension table.

WITH order_items AS (
    SELECT * FROM {{ ref('stg_order_items') }}
),

orders AS (
    SELECT * FROM {{ ref('stg_orders') }}
),

products AS (
    SELECT * FROM {{ ref('stg_products') }}
),

shipments AS (
    SELECT * FROM {{ ref('stg_shipments') }}
),

customers AS (
    SELECT * FROM {{ ref('stg_customers') }}
)

SELECT
    -- Surrogate key for this fact row: combination of item and order IDs.
    -- We concatenate them to create a unique identifier for each row.
    oi.item_id                                         AS fact_id,

    -- Foreign keys — these link to the dimension tables.
    -- An analyst can JOIN fact_sales to dim_customers ON customer_id,
    -- and instantly get the customer's name, region, etc.
    o.customer_id,
    oi.product_id,
    o.order_date                                       AS date_id,  -- links to dim_date
    o.order_id,
    oi.item_id,

    -- Order status (useful for filtering: only look at completed orders)
    o.status                                           AS order_status,

    -- Measures — the numbers we want to analyse
    oi.quantity,
    oi.unit_price,
    oi.line_revenue,

    -- Profit = revenue minus cost. We get cost from the products staging table.
    -- ROUND() keeps it to 2 decimal places so we don't get floating point noise.
    ROUND(oi.line_revenue - (p.cost_price * oi.quantity), 2) AS line_profit,

    -- Profit margin as a percentage.
    -- We guard against division by zero with NULLIF — if line_revenue is 0,
    -- return NULL instead of crashing with a "division by zero" error.
    ROUND(
        (oi.line_revenue - (p.cost_price * oi.quantity))
        / NULLIF(oi.line_revenue, 0) * 100,
    2)                                                 AS profit_margin_pct,

    -- Shipping days: how long did delivery take?
    -- We calculate it from the shipments table by subtracting dates.
    s.dispatch_date,
    s.delivery_date,
    (s.delivery_date - o.order_date)                  AS shipping_days,
    s.carrier,

    NOW()                                              AS dbt_loaded_at

FROM order_items oi

-- JOIN to orders to get customer, date, status
INNER JOIN orders o
    ON oi.order_id = o.order_id

-- JOIN to products to get cost_price for profit calculation
INNER JOIN products p
    ON oi.product_id = p.product_id

-- LEFT JOIN to shipments — not every order has a shipment record yet
-- (e.g. orders placed today won't have a logistics file until tomorrow).
-- LEFT JOIN keeps all order items even if there's no matching shipment,
-- whereas INNER JOIN would silently drop them.
LEFT JOIN shipments s
    ON oi.order_id = s.order_id

-- We only include completed and shipped orders in the fact table.
-- Cancelled orders shouldn't appear in a sales analysis.
WHERE o.status IN ('completed', 'shipped')