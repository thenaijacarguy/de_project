import psycopg2
from dotenv import load_dotenv
import os

load_dotenv()

conn = psycopg2.connect(
    host = os.getenv("WAREHOUSE_DB_HOST"),
    port = os.getenv("WAREHOUSE_DB_PORT"),
    dbname = os.getenv("WAREHOUSE_DB_NAME"),
    user = os.getenv("WAREHOUSE_DB_USER"),
    password = os.getenv("WAREHOUSE_DB_PASS")
)
conn.autocommit = True
cur = conn.cursor()

# Create the three schema layers
cur.execute("CREATE SCHEMA IF NOT EXISTS raw;")       # untouched source copies
cur.execute("CREATE SCHEMA IF NOT EXISTS staging;")   # cleaned, typed tables
cur.execute("CREATE SCHEMA IF NOT EXISTS marts;")     # star schema for analytics

print("✅ Schemas created: raw, staging, marts")
cur.close()
conn.close()