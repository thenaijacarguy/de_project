import psycopg2
import random
from datetime import date, timedelta
from dotenv import load_dotenv
import os

load_dotenv()

conn = psycopg2.connect(
    host=os.getenv("SOURCE_DB_HOST"),
    port=os.getenv("SOURCE_DB_PORT"),
    dbname=os.getenv("SOURCE_DB_NAME"),
    user=os.getenv("SOURCE_DB_USER"),
    password=os.getenv("SOURCE_DB_PASS")
)
conn.autocommit = True
cur = conn.cursor()

# --- Create tables ---
cur.execute("""
CREATE TABLE IF NOT EXISTS customers (
    customer_id VARCHAR(20) PRIMARY KEY,
    name VARCHAR(100),
    email VARCHAR(100),
    signup_date DATE,
    region VARCHAR(20)
);

CREATE TABLE IF NOT EXISTS products (
    product_id VARCHAR(20) PRIMARY KEY,
    name VARCHAR(100),
    category VARCHAR(50),
    cost_price NUMERIC(10,2)
);

CREATE TABLE IF NOT EXISTS orders (
    order_id VARCHAR(20) PRIMARY KEY,
    customer_id VARCHAR(20) REFERENCES customers(customer_id),
    order_date DATE,
    status VARCHAR(20),
    total_amount NUMERIC(10,2)
);

CREATE TABLE IF NOT EXISTS order_items (
    item_id VARCHAR(20) PRIMARY KEY,
    order_id VARCHAR(20) REFERENCES orders(order_id),
    product_id VARCHAR(20) REFERENCES products(product_id),
    quantity INTEGER,
    unit_price NUMERIC(10,2)
);
""")

# --- Seed data ---
regions = ['North', 'South', 'East', 'West']
categories = ['Electronics', 'Clothing', 'Home', 'Food']
statuses = ['completed', 'completed', 'completed', 'shipped', 'cancelled']  # weighted

# Customers
customers = []
for i in range(1, 201):
    cid = f"CUST{i:04d}"
    customers.append(cid)
    signup = date(2023, 1, 1) + timedelta(days=random.randint(0, 365))
    cur.execute(
        "INSERT INTO customers VALUES (%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING",
        (cid, f"Customer {i}", f"customer{i}@email.com", signup, random.choice(regions))
    )

# Products
products = []
for i in range(1, 51):
    pid = f"PROD{i:04d}"
    products.append(pid)
    cat = random.choice(categories)
    cost = round(random.uniform(5, 200), 2)
    cur.execute(
        "INSERT INTO products VALUES (%s,%s,%s,%s) ON CONFLICT DO NOTHING",
        (pid, f"Product {i}", cat, cost)
    )

# Orders + Order Items
for i in range(1, 501):
    oid = f"ORD{i:05d}"
    cid = random.choice(customers)
    odate = date(2024, 1, 1) + timedelta(days=random.randint(0, 364))
    status = random.choice(statuses)
    total = round(random.uniform(20, 500), 2)
    cur.execute(
        "INSERT INTO orders VALUES (%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING",
        (oid, cid, odate, status, total)
    )
    # 1–3 items per order
    for j in range(1, random.randint(2, 4)):
        iid = f"ITEM{i:05d}{j}"
        pid = random.choice(products)
        qty = random.randint(1, 5)
        price = round(random.uniform(10, 150), 2)
        cur.execute(
            "INSERT INTO order_items VALUES (%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING",
            (iid, oid, pid, qty, price)
        )

print(" Source database seeded: 200 customers, 50 products, 500 orders")
cur.close()
conn.close()