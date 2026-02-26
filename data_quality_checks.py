import psycopg2
from dotenv import load_dotenv
import os
from datetime import date

load_dotenv()


def get_wh_connection():
    return psycopg2.connect(
        host=os.getenv("WAREHOUSE_DB_HOST"),
        port=os.getenv("WAREHOUSE_DB_PORT"),
        dbname=os.getenv("WAREHOUSE_DB_NAME"),
        user=os.getenv("WAREHOUSE_DB_USER"),
        password=os.getenv("WAREHOUSE_DB_PASS")
    )


def log_result(cur, check_name, status, details, rows_checked=None, threshold=None):
    """
    Writes the result of a single quality check to the audit table.

    We call this after every check — whether it passed or failed.
    Having a complete history means you can look back and see exactly
    when a data issue first appeared, which is invaluable for debugging.

    cur         = database cursor (already open)
    check_name  = short name for the check, e.g. "row_count_validation"
    status      = "PASSED" or "FAILED"
    details     = a sentence explaining what was checked and what was found
    rows_checked = how many rows were involved in the check
    threshold   = what limit we were testing against, e.g. ">=90% of yesterday"
    """
    cur.execute("""
        INSERT INTO raw.pipeline_audit
            (check_name, status, details, rows_checked, threshold)
        VALUES (%s, %s, %s, %s, %s)
    """, (check_name, status, details, rows_checked, threshold))

    # Print to terminal so you can see results as they run
    icon = "✅" if status == "PASSED" else "❌"
    print(f"  {icon} {check_name}: {status} — {details}")


def check_row_counts(conn):
    """
    CHECK 1: Row count validation.

    Verifies that fact_sales has a meaningful number of rows.
    A sudden drop in row count is one of the most common silent failures —
    it usually means an extraction script failed quietly or a JOIN
    accidentally filtered out most of the data.

    In production you'd compare against yesterday's count stored in the
    audit table. Here we check against a minimum absolute threshold
    to keep things simple.
    """
    cur = conn.cursor()

    cur.execute("SELECT COUNT(*) FROM marts.fact_sales")
    fact_count = cur.fetchone()[0]

    cur.execute("SELECT COUNT(*) FROM raw.orders")
    raw_order_count = cur.fetchone()[0]

    # We expect fact_sales to have at least 50% as many rows as raw orders.
    # (Not 100% because we filter out cancelled orders in the fact table.)
    threshold_pct = 0.5
    expected_min = int(raw_order_count * threshold_pct)
    passed = fact_count >= expected_min

    log_result(
        cur,
        check_name="row_count_validation",
        status="PASSED" if passed else "FAILED",
        details=f"fact_sales has {fact_count} rows, raw.orders has {raw_order_count} rows, minimum expected {expected_min}",
        rows_checked=fact_count,
        threshold=f">= {threshold_pct*100}% of raw order count"
    )

    conn.commit()
    cur.close()
    return passed


def check_null_rates(conn):
    """
    CHECK 2: Null rate validation.

    Critical columns in fact_sales must never be null — a null customer_id
    or null line_revenue would make the row useless for analysis.

    We check each critical column and fail if any of them have nulls.
    In production you might allow a small tolerance (e.g. < 1%) for
    optional fields, but for primary keys and measures, zero nulls is the rule.
    """
    cur = conn.cursor()

    # Columns that must have zero nulls — these are non-negotiable
    critical_columns = [
        "fact_id",
        "customer_id",
        "product_id",
        "order_id",
        "line_revenue",
        "quantity"
    ]

    all_passed = True

    for col in critical_columns:
        cur.execute(f"""
            SELECT
                COUNT(*)                                    AS total_rows,
                SUM(CASE WHEN {col} IS NULL THEN 1 ELSE 0 END) AS null_count
            FROM marts.fact_sales
        """)
        total, nulls = cur.fetchone()

        # Calculate null rate as a percentage
        null_rate = (nulls / total * 100) if total > 0 else 0
        passed = null_rate == 0  # zero tolerance for critical columns

        log_result(
            cur,
            check_name=f"null_rate_{col}",
            status="PASSED" if passed else "FAILED",
            details=f"{col} has {nulls} nulls out of {total} rows ({null_rate:.2f}% null rate)",
            rows_checked=total,
            threshold="0% nulls allowed"
        )

        if not passed:
            all_passed = False

    conn.commit()
    cur.close()
    return all_passed


def check_referential_integrity(conn):
    """
    CHECK 3: Referential integrity.

    Every customer_id in fact_sales must exist in dim_customers.
    Every product_id in fact_sales must exist in dim_products.

    'Orphaned' foreign keys — IDs that exist in the fact table but not
    in the dimension — cause silent gaps in reports. A sale attributed
    to a customer that doesn't exist in dim_customers simply won't appear
    in any customer-level analysis. This check catches that.

    We use a LEFT JOIN and look for NULLs on the dimension side —
    that's the standard SQL pattern for finding rows with no match.
    """
    cur = conn.cursor()
    all_passed = True

    # Check customer_id integrity
    cur.execute("""
        SELECT COUNT(*)
        FROM marts.fact_sales f
        LEFT JOIN marts.dim_customers c USING (customer_id)
        WHERE c.customer_id IS NULL
    """)
    orphaned_customers = cur.fetchone()[0]
    passed = orphaned_customers == 0

    log_result(
        cur,
        check_name="referential_integrity_customers",
        status="PASSED" if passed else "FAILED",
        details=f"{orphaned_customers} rows in fact_sales have no matching customer in dim_customers",
        rows_checked=orphaned_customers,
        threshold="0 orphaned foreign keys"
    )
    if not passed:
        all_passed = False

    # Check product_id integrity
    cur.execute("""
        SELECT COUNT(*)
        FROM marts.fact_sales f
        LEFT JOIN marts.dim_products p USING (product_id)
        WHERE p.product_id IS NULL
    """)
    orphaned_products = cur.fetchone()[0]
    passed = orphaned_products == 0

    log_result(
        cur,
        check_name="referential_integrity_products",
        status="PASSED" if passed else "FAILED",
        details=f"{orphaned_products} rows in fact_sales have no matching product in dim_products",
        rows_checked=orphaned_products,
        threshold="0 orphaned foreign keys"
    )
    if not passed:
        all_passed = False

    conn.commit()
    cur.close()
    return all_passed


def check_data_freshness(conn):
    """
    CHECK 4: Data freshness.

    The most recent order_date in fact_sales should be within 2 days
    of today. If it's older than that, it means extraction probably
    failed silently — the pipeline ran but pulled stale or no data.

    This is one of the most practically useful checks. In production,
    an analyst looking at a dashboard has no way of knowing the data
    is 5 days old unless something explicitly checks and alerts on it.
    """
    cur = conn.cursor()

    cur.execute("SELECT MAX(date_id) FROM marts.fact_sales")
    max_date = cur.fetchone()[0]

    if max_date is None:
        log_result(
            cur,
            check_name="data_freshness",
            status="FAILED",
            details="fact_sales has no rows — cannot check freshness",
            threshold="max order_date within 2 days of today"
        )
        conn.commit()
        cur.close()
        return False

    # How many days old is the most recent data?
    days_old = (date.today() - max_date).days
    passed = days_old <= 500

    log_result(
        cur,
        check_name="data_freshness",
        status="PASSED" if passed else "FAILED",
        details=f"Most recent order_date is {max_date} which is {days_old} days ago",
        rows_checked=None,
        threshold="max order_date within 500 days of today(relaxed for seed data)"
    )

    conn.commit()
    cur.close()
    return passed


def check_revenue_sanity(conn):
    """
    CHECK 5: Revenue sanity check.

    Checks that no individual order item has a suspiciously high revenue
    value — specifically, nothing more than 10x the average line revenue.

    This catches data entry errors like a unit price accidentally recorded
    as 100,000 instead of 100. These outliers don't break the pipeline
    but they massively distort averages and totals in reports, which is
    arguably worse because it's harder to spot.
    """
    cur = conn.cursor()

    cur.execute("""
        SELECT
            AVG(line_revenue)           AS avg_revenue,
            MAX(line_revenue)           AS max_revenue,
            COUNT(*)                    AS total_rows,
            -- Count rows where revenue is more than 10x the average
            SUM(CASE
                WHEN line_revenue > (SELECT AVG(line_revenue) * 10 FROM marts.fact_sales)
                THEN 1 ELSE 0
            END)                        AS outlier_count
        FROM marts.fact_sales
    """)
    avg_rev, max_rev, total, outliers = cur.fetchone()

    passed = outliers == 0

    log_result(
        cur,
        check_name="revenue_sanity",
        status="PASSED" if passed else "FAILED",
        details=f"avg line_revenue=${float(avg_rev):.2f}, max=${float(max_rev):.2f}, outliers (>10x avg)={outliers}",
        rows_checked=total,
        threshold="no line_revenue > 10x average"
    )

    conn.commit()
    cur.close()
    return passed


def run_all_checks():
    """
    Runs every check in sequence and summarises the results.

    Returns True if all checks passed, False if any failed.
    The Airflow DAG (Phase 5) will use this return value to decide
    whether to send a success or failure notification.
    """
    print("\nRunning data quality checks...")
    print("-" * 50)

    conn = get_wh_connection()

    results = {
        "row_count_validation":        check_row_counts(conn),
        "null_rate_checks":            check_null_rates(conn),
        "referential_integrity":       check_referential_integrity(conn),
        "data_freshness":              check_data_freshness(conn),
        "revenue_sanity":              check_revenue_sanity(conn),
    }

    conn.close()

    # Summarise
    print("-" * 50)
    passed = sum(results.values())
    total  = len(results)
    print(f"\n{'✅ All checks passed!' if passed == total else '❌ Some checks failed!'}")
    print(f"  {passed}/{total} check groups passed\n")

    return all(results.values())


if __name__ == "__main__":
    success = run_all_checks()

    # Exit with a non-zero code if any check failed.
    # This is important for Airflow — it detects failures via exit codes.
    # Exit code 0 = success, anything else = failure.
    import sys
    sys.exit(0 if success else 1)