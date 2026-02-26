# Data Dictionary

## raw schema

Unmodified source data. All columns stored as TEXT.
Never modify these tables directly — they are overwritten on each run.

### raw.orders
Source: PostgreSQL transactional database

| Column | Description |
|--------|-------------|
| order_id | Unique order identifier |
| customer_id | Foreign key to customers |
| order_date | Date the order was placed |
| status | Order status: completed, shipped, cancelled |
| total_amount | Total order value in USD |

### raw.order_items
Source: PostgreSQL transactional database

| Column | Description |
|--------|-------------|
| item_id | Unique line item identifier |
| order_id | Foreign key to orders |
| product_id | Foreign key to products |
| quantity | Number of units ordered |
| unit_price | Price per unit at time of order |

### raw.customers
Source: PostgreSQL transactional database

| Column | Description |
|--------|-------------|
| customer_id | Unique customer identifier |
| name | Customer full name |
| email | Customer email address |
| signup_date | Date customer registered |
| region | Sales region: North, South, East, West |

### raw.products
Source: PostgreSQL transactional database

| Column | Description |
|--------|-------------|
| product_id | Unique product identifier |
| name | Product display name |
| category | Product category |
| cost_price | Cost to the business per unit |

### raw.shipments
Source: Logistics provider CSV

| Column | Description |
|--------|-------------|
| shipment_id | Provider's internal shipment ID |
| order_reference | Maps to orders.order_id (may have prefix) |
| dispatch_date | Date shipment left warehouse (mixed formats) |
| delivery_date | Date delivered — blank if in transit |
| carrier | Shipping carrier name (inconsistent casing) |
| status | DELIVERED, IN_TRANSIT, FAILED, RETURNED |
| weight_kg | Shipment weight — may include 'kg' suffix |
| source_file | Name of the CSV file this row came from |

### raw.campaigns
Source: Marketing REST API

| Column | Description |
|--------|-------------|
| fetched_date | Date the API was queried for |
| raw_response | Full JSON response body as text |
| loaded_at | Timestamp when this row was loaded |

---

## staging schema

Cleaned and typed views built by dbt. One view per raw table.
All columns have correct data types. No joins across sources.

### staging.stg_customers

| Column | Type | Description |
|--------|------|-------------|
| customer_id | TEXT | Lowercased, trimmed |
| customer_name | TEXT | Title-cased |
| email | TEXT | Lowercased, trimmed |
| signup_date | DATE | Cast from text |
| region | TEXT | Trimmed |
| dbt_loaded_at | TIMESTAMP | When dbt last processed this row |

### staging.stg_orders

| Column | Type | Description |
|--------|------|-------------|
| order_id | TEXT | Trimmed |
| customer_id | TEXT | Lowercased, trimmed |
| order_date | DATE | Cast from text |
| status | TEXT | Lowercased — test/internal orders excluded |
| total_amount | NUMERIC(10,2) | Cast from text |
| dbt_loaded_at | TIMESTAMP | When dbt last processed this row |

### staging.stg_order_items

| Column | Type | Description |
|--------|------|-------------|
| item_id | TEXT | Trimmed |
| order_id | TEXT | Trimmed |
| product_id | TEXT | Lowercased, trimmed |
| quantity | INTEGER | Cast from text — rows with quantity <= 0 excluded |
| unit_price | NUMERIC(10,2) | Cast from text |
| line_revenue | NUMERIC | quantity × unit_price |
| dbt_loaded_at | TIMESTAMP | When dbt last processed this row |

### staging.stg_shipments

| Column | Type | Description |
|--------|------|-------------|
| shipment_id | TEXT | Trimmed |
| order_id | TEXT | REF- prefix stripped to match orders.order_id |
| dispatch_date | DATE | Normalised from MM/DD/YYYY or YYYY-MM-DD |
| delivery_date | DATE | Normalised — NULL if not yet delivered |
| carrier | TEXT | Uppercased and trimmed |
| status | TEXT | Uppercased and trimmed |
| weight_kg | NUMERIC(10,2) | 'kg' suffix stripped |
| source_file | TEXT | Source CSV filename |
| dbt_loaded_at | TIMESTAMP | When dbt last processed this row |

---

## marts schema

Star schema tables for analytics consumption.
Query these tables directly for reporting and analysis.

### marts.fact_sales
Grain: one row per order line item.
This is the central table — join to dimension tables for context.

| Column | Type | Description |
|--------|------|-------------|
| fact_id | TEXT | Primary key — unique per order item |
| customer_id | TEXT | FK → dim_customers.customer_id |
| product_id | TEXT | FK → dim_products.product_id |
| date_id | DATE | FK → dim_date.date_id |
| order_id | TEXT | The parent order |
| item_id | TEXT | The specific line item |
| order_status | TEXT | completed or shipped |
| quantity | INTEGER | Units sold |
| unit_price | NUMERIC | Price per unit |
| line_revenue | NUMERIC | Total revenue for this line item |
| line_profit | NUMERIC | Revenue minus cost |
| profit_margin_pct | NUMERIC | Profit as % of revenue |
| dispatch_date | DATE | When shipment was dispatched |
| delivery_date | DATE | When shipment was delivered (NULL if pending) |
| shipping_days | INTEGER | Days from order to delivery |
| carrier | TEXT | Shipping carrier |
| dbt_loaded_at | TIMESTAMP | When dbt last processed this row |

### marts.dim_customers

| Column | Type | Description |
|--------|------|-------------|
| customer_id | TEXT | Primary key |
| customer_name | TEXT | Full name |
| email | TEXT | Email address |
| signup_date | DATE | Registration date |
| region | TEXT | Sales region |
| days_since_signup | NUMERIC | Days between signup and today |
| dbt_loaded_at | TIMESTAMP | When dbt last processed this row |

### marts.dim_products

| Column | Type | Description |
|--------|------|-------------|
| product_id | TEXT | Primary key |
| product_name | TEXT | Display name |
| category | TEXT | Product category |
| cost_price | NUMERIC | Cost to business per unit |
| dbt_loaded_at | TIMESTAMP | When dbt last processed this row |

### marts.dim_date

| Column | Type | Description |
|--------|------|-------------|
| date_id | DATE | Primary key |
| full_date | DATE | Same as date_id |
| year | INTEGER | Calendar year |
| month_number | INTEGER | 1–12 |
| month_name | TEXT | January, February etc |
| quarter | INTEGER | 1–4 |
| day_of_week | INTEGER | 0=Sunday, 6=Saturday |
| day_name | TEXT | Monday, Tuesday etc |
| is_weekend | BOOLEAN | True if Saturday or Sunday |

### raw.pipeline_audit

| Column | Type | Description |
|--------|------|-------------|
| id | SERIAL | Auto-incrementing row ID |
| checked_at | TIMESTAMP | When the check ran |
| check_name | TEXT | Name of the quality check |
| status | TEXT | PASSED or FAILED |
| details | TEXT | Human-readable explanation |
| rows_checked | INTEGER | Number of rows evaluated |
| threshold | TEXT | The limit being tested against |