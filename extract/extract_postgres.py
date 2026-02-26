import psycopg2
import pandas as pd
from dotenv import load_dotenv
import os

load_dotenv()


def get_connection(host, port, dbname, user, password):
    """
    A small helper function that creates and returns a database connection.
    We define this once and reuse it instead of repeating the same
    psycopg2.connect() call multiple times. This keeps the code DRY
    (Don't Repeat Yourself).
    """
    return psycopg2.connect(
        host=host,
        port=port,
        dbname=dbname,
        user=user,
        password=password
    )


def ensure_raw_table_exists(wh_conn, table_name, source_conn):
    """
    Before we can write data into the warehouse, the destination table
    needs to exist. This function checks whether it does, and if not,
    it creates it automatically by copying the column structure from
    the source table.

    This is called 'schema inference': we don't hardcode the column
    definitions, we let Postgres tell us what columns exist in the source.

    wh_conn     = connection to your warehouse database
    table_name  = e.g. 'orders', 'customers'
    source_conn = connection to the source database (so we can read its schema)
    """
    wh_cur = wh_conn.cursor()

    wh_cur.execute("""
        SELECT COUNT(*) FROM information_schema.tables
        WHERE table_schema = 'raw' AND table_name = %s
    """, (table_name,))

    exists = wh_cur.fetchone()[0] > 0  # fetchone() returns a single row tuple

    if not exists:
        src_cur = source_conn.cursor()
        src_cur.execute("""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_name = %s
            ORDER BY ordinal_position
        """, (table_name,))

        
        columns = ", ".join([f"{col} text" for col, dtype in src_cur.fetchall()])
       

        wh_cur.execute(f"CREATE TABLE raw.{table_name} ({columns})")
        wh_conn.commit()
        print(f"   Created raw.{table_name}")

    wh_cur.close()


def extract_table(table_name, source_conn, wh_conn):
    """
    The main extraction function. It:
    1. Makes sure the destination table exists (calls ensure_raw_table_exists)
    2. Deletes whatever was there before (full refresh strategy)
    3. Reads all rows from the source table into a pandas DataFrame
    4. Writes those rows into the warehouse raw table

    We're doing a 'full refresh' here: truncate and reload everything
    each run. For small tables (customers, products) this is fine.
    For very large tables in production you'd use incremental loading
    (only pull new/changed rows), but full refresh is simpler to start.
    """
    print(f"  Extracting: {table_name}")

    ensure_raw_table_exists(wh_conn, table_name, source_conn)

    
    df = pd.read_sql(f"SELECT * FROM {table_name}", source_conn)

    wh_cur = wh_conn.cursor()

    
    wh_cur.execute(f"TRUNCATE TABLE raw.{table_name}")

    if len(df) > 0:
       
        records = [tuple(row) for row in df.values]

        
        placeholders = ", ".join(["%s"] * len(df.columns))
        insert_sql = f"INSERT INTO raw.{table_name} VALUES ({placeholders})"

        
        wh_cur.executemany(insert_sql, records)

    
    wh_conn.commit()

    print(f"   {table_name}: {len(df)} rows loaded into raw.{table_name}")
    wh_cur.close()



if __name__ == "__main__":
    print("Starting PostgreSQL extraction...")

    
    source_conn = get_connection(
        os.getenv("SOURCE_DB_HOST"),
        os.getenv("SOURCE_DB_PORT"),
        os.getenv("SOURCE_DB_NAME"),
        os.getenv("SOURCE_DB_USER"),
        os.getenv("SOURCE_DB_PASS")
    )

    wh_conn = get_connection(
        os.getenv("WAREHOUSE_DB_HOST"),
        os.getenv("WAREHOUSE_DB_PORT"),
        os.getenv("WAREHOUSE_DB_NAME"),
        os.getenv("WAREHOUSE_DB_USER"),
        os.getenv("WAREHOUSE_DB_PASS")
    )

    
    for table in ["customers", "products", "orders", "order_items"]:
        extract_table(table, source_conn, wh_conn)

    
    source_conn.close()
    wh_conn.close()

    print("\n PostgreSQL extraction complete.")