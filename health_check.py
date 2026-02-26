import psycopg2
from dotenv import load_dotenv
import os

load_dotenv()

def check_db(name, host, port, dbname, user, password):
    try:
        conn = psycopg2.connect(host=host, port=port, dbname=dbname, user=user, password=password)
        cur = conn.cursor()
        cur.execute("SELECT 1")
        conn.close()
        print(f"✅ {name}: connected")
    except Exception as e:
        print(f"❌ {name}: FAILED — {e}")

check_db("Source DB",    os.getenv("SOURCE_DB_HOST"), os.getenv("SOURCE_DB_PORT"),
         os.getenv("SOURCE_DB_NAME"), os.getenv("SOURCE_DB_USER"), os.getenv("SOURCE_DB_PASS"))

check_db("Warehouse DB", os.getenv("WAREHOUSE_DB_HOST"), os.getenv("WAREHOUSE_DB_PORT"),
         os.getenv("WAREHOUSE_DB_NAME"), os.getenv("WAREHOUSE_DB_USER"), os.getenv("WAREHOUSE_DB_PASS"))

print("\n✅ Phase 1 complete — environment is ready!")