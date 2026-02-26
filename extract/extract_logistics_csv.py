import pandas as pd
import psycopg2
from dotenv import load_dotenv
import os

load_dotenv()


def get_wh_connection():
    """
    Same helper pattern as before — create and return a warehouse connection.
    We only connect to the warehouse here because CSV files aren't a database;
    we read them with pandas directly from the filesystem.
    """
    return psycopg2.connect(
        host=os.getenv("WAREHOUSE_DB_HOST"),
        port=os.getenv("WAREHOUSE_DB_PORT"),
        dbname=os.getenv("WAREHOUSE_DB_NAME"),
        user=os.getenv("WAREHOUSE_DB_USER"),
        password=os.getenv("WAREHOUSE_DB_PASS")
    )


def ensure_shipments_table(wh_conn):
    """
    Unlike the Postgres extraction where we inferred columns from the source,
    here we know exactly what columns the CSV has (the logistics provider
    gave us a spec document). So we hardcode the CREATE TABLE statement.

    Everything is stored as TEXT — raw means raw, no type enforcement yet.
    """
    cur = wh_conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS raw.shipments (
            shipment_id        TEXT,
            order_reference    TEXT,
            dispatch_date      TEXT,
            delivery_date      TEXT,
            carrier            TEXT,
            status             TEXT,
            weight_kg          TEXT,
            source_file        TEXT  -- extra column: tracks which CSV file this row came from
        )
    """)
    # Notice we added a 'source_file' column that doesn't exist in the CSV.
    # This is a common pattern — adding metadata columns that help with
    # debugging and auditing. If a row looks wrong, you know exactly which
    # file it came from.
    wh_conn.commit()
    cur.close()


def extract_csv(file_path, wh_conn):
    """
    Reads a single CSV file and loads all its rows into raw.shipments.

    file_path = path to the CSV file, e.g. "data/csv/shipments_20240115.csv"
    wh_conn   = warehouse connection
    """
    print(f"  Reading file: {file_path}")

    # Check the file actually exists before trying to read it.
    # In production, you'd check an SFTP server here instead.
    if not os.path.exists(file_path):
        print(f"  ⚠️  File not found: {file_path} — skipping")
        return

    # pd.read_csv() reads the CSV file into a DataFrame.
    # dtype=str means "treat every column as a string, don't try to
    # infer types automatically." We want raw text — no automatic
    # conversion of dates or numbers, because pandas might silently
    # misinterpret the messy formats in this file.
    # keep_default_na=False means empty cells become empty strings ""
    # rather than NaN (Not a Number), which is easier to handle later.
    df = pd.read_csv(file_path, dtype=str, keep_default_na=False)

    # Add the metadata column — the filename without its directory path
    df['source_file'] = os.path.basename(file_path)

    # Strip whitespace from column names, in case the CSV has
    # " shipment_id" with a leading space (a surprisingly common issue)
    df.columns = df.columns.str.strip()

    cur = wh_conn.cursor()

    # We DON'T truncate here like we did for the Postgres tables.
    # Why? Because there could be multiple CSV files (one per day), and we
    # want to keep all of them. Instead we load the file and let the
    # staging layer handle deduplication.
    # However, we do check if this specific file was already loaded,
    # to avoid loading the same file twice if the script is run again.
    cur.execute(
        "SELECT COUNT(*) FROM raw.shipments WHERE source_file = %s",
        (os.path.basename(file_path),)
    )
    already_loaded = cur.fetchone()[0] > 0

    if already_loaded:
        print(f"  ⏭️  Already loaded — skipping {os.path.basename(file_path)}")
        cur.close()
        return

    # Load each row into the warehouse
    records = [tuple(row) for row in df.values]
    placeholders = ", ".join(["%s"] * len(df.columns))
    cur.executemany(
        f"INSERT INTO raw.shipments VALUES ({placeholders})",
        records
    )
    wh_conn.commit()

    print(f"  ✅ Loaded {len(df)} rows from {os.path.basename(file_path)}")
    cur.close()


if __name__ == "__main__":
    print("Starting logistics CSV extraction...")

    wh_conn = get_wh_connection()
    ensure_shipments_table(wh_conn)

    # In a real pipeline, you'd scan an SFTP folder for all new files.
    # Here we just specify our sample file directly.
    csv_files = ["data/csv/shipment.csv"]

    for f in csv_files:
        extract_csv(f, wh_conn)

    wh_conn.close()
    print("\n✅ Logistics CSV extraction complete.")