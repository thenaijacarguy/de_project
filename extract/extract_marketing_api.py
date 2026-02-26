import requests
import psycopg2
import json
import time
from datetime import date
from dotenv import load_dotenv
import os

load_dotenv()


def get_wh_connection():
    return psycopg2.connect(
        host=os.getenv("WAREHOUSE_DB_HOST"),
        port=os.getenv("WAREHOUSE_DB_PORT"),
        dbname=os.getenv("WAREHOUSE_DB_NAME"),
        user=os.getenv("WAREHOUSE_DB_USER"),
        password=os.getenv("WAREHOUSE_DB_PASS")
    )


def ensure_campaigns_table(wh_conn):
    """
    Creates the raw campaigns table if it doesn't exist.
    Notice we store the entire API response as a single JSON text column.
    This is called 'raw JSON storage': we preserve the exact API response
    so if the schema changes later or we need to re-process, we have the
    original data. The staging layer will unpack the JSON into proper columns.
    """
    cur = wh_conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS raw.campaigns (
            fetched_date   TEXT,    -- the date we requested data for
            raw_response   TEXT,    -- the full JSON response as a string
            loaded_at      TIMESTAMP DEFAULT NOW()  -- when we loaded it
        )
    """)
    wh_conn.commit()
    cur.close()


def fetch_with_retry(url, headers, max_retries=3):
    """
    Makes an HTTP GET request and retries if it fails.

    APIs can fail for many reasons: network hiccup, server overloaded,
    rate limit hit (HTTP 429), temporary outage. Rather than crashing
    your pipeline on the first failure, we retry a few times with
    increasing wait times between attempts.

    This pattern is called 'exponential backoff':
    - Wait 2 seconds before retry 1
    - Wait 4 seconds before retry 2
    - Wait 8 seconds before retry 3
    If all retries fail, we raise an exception and the pipeline stops.

    url     = the API endpoint to call
    headers = HTTP headers (authentication goes here)
    max_retries = how many times to retry before giving up
    """
    for attempt in range(max_retries):
        try:
            # requests.get() makes an HTTP GET request to the URL.
            # timeout=10 means: if the server doesn't respond within
            # 10 seconds, stop waiting and raise an error.
            response = requests.get(url, headers=headers, timeout=10)

            # HTTP 429 means "Too Many Requests" — the API is rate limiting us.
            # We wait longer and retry instead of treating this as a hard error.
            if response.status_code == 429:
                wait = 2 ** attempt  # 1→2s, 2→4s, 3→8s
                print(f"  ⏳ Rate limited. Waiting {wait}s before retry {attempt+1}/{max_retries}...")
                time.sleep(wait)
                continue  # skip to the next loop iteration (retry)

            # raise_for_status() raises an exception for any HTTP error
            # status code (4xx client errors, 5xx server errors).
            # 200 OK is fine — anything else raises an error here.
            response.raise_for_status()

            # If we got here, the request succeeded. Return the response.
            return response

        except requests.exceptions.RequestException as e:
            # This catches all requests errors: connection refused,
            # timeout, DNS failure, etc.
            if attempt < max_retries - 1:
                wait = 2 ** attempt
                print(f"  ⚠️  Request failed: {e}. Retrying in {wait}s...")
                time.sleep(wait)
            else:
                # Last attempt also failed — raise the error so the
                # pipeline knows extraction failed for this date
                raise


def extract_campaigns(target_date, wh_conn):
    """
    Fetches campaign performance data for a specific date from the API
    and stores the raw response in the warehouse.

    target_date = a date object, e.g. date(2024, 1, 15)
    """
    date_str = target_date.strftime("%Y-%m-%d")
    print(f"  Fetching campaigns for date: {date_str}")

    cur = wh_conn.cursor()

    # Idempotency check: don't fetch the same date twice.
    # 'Idempotent' means running the same operation multiple times
    # produces the same result as running it once. This is critical
    # in pipelines — if a script crashes halfway and reruns, you don't
    # want duplicate data.
    cur.execute(
        "SELECT COUNT(*) FROM raw.campaigns WHERE fetched_date = %s",
        (date_str,)
    )
    if cur.fetchone()[0] > 0:
        print(f"  ⏭️  Already fetched data for {date_str} — skipping")
        cur.close()
        return

    # Build the API request.
    # The Bearer token is how the API authenticates us — it's like a
    # password that goes in the HTTP header, not in the URL.
    base_url = os.getenv("MARKETING_API_URL")
    token = os.getenv("MARKETING_API_TOKEN")
    url = f"{base_url}/posts"  # Using JSONPlaceholder as our mock API
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

    # Make the request (with automatic retries)
    response = fetch_with_retry(url, headers)

    # response.json() parses the JSON response body into a Python
    # list or dictionary. json.dumps() converts it back to a string
    # for storage — we want to store the raw JSON text, not a Python object.
    raw_json = json.dumps(response.json())

    # Store the raw JSON response with the date it belongs to
    cur.execute(
        "INSERT INTO raw.campaigns (fetched_date, raw_response) VALUES (%s, %s)",
        (date_str, raw_json)
    )
    wh_conn.commit()
    print(f"  ✅ Stored campaign response for {date_str}")
    cur.close()


if __name__ == "__main__":
    print("Starting marketing API extraction...")

    wh_conn = get_wh_connection()
    ensure_campaigns_table(wh_conn)

    extract_campaigns(date.today(), wh_conn)

    wh_conn.close()
    print("\n Marketing API extraction complete.")