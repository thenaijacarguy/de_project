# Sales Data Pipeline

![Python](https://img.shields.io/badge/Python-3.10+-blue)
![dbt](https://img.shields.io/badge/dbt-1.11-orange)
![Airflow](https://img.shields.io/badge/Airflow-2.7-red)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15-blue)
![Docker](https://img.shields.io/badge/Docker-Compose-2496ED)
![Status](https://img.shields.io/badge/Status-Complete-brightgreen)

An end-to-end data engineering pipeline that consolidates sales data from three sources — a transactional PostgreSQL database, a logistics CSV file drop, and a marketing REST API — into a single analytics-ready data warehouse built on a star schema.

---

## Table of Contents

- [Architecture](#architecture)
- [Tech Stack](#tech-stack)
- [Project Structure](#project-structure)
- [Prerequisites](#prerequisites)
- [Setup Instructions](#setup-instructions)
- [Data Sources](#data-sources)
- [Warehouse Schema](#warehouse-schema)
- [Data Quality Checks](#data-quality-checks)
- [Airflow DAG](#airflow-dag)
- [Adding a New Data Source](#adding-a-new-data-source)
- [Known Limitations](#known-limitations)
- [Troubleshooting](#troubleshooting)
- [Key Gotchas](#key-gotchas)
- [What I Learned](#what-i-learned)
- [Version History](#version-history)
- [Contributors](#contributors)
- [Licence](#licence)

---

## Architecture
```
┌──────────────────────────────────────────────────────┐
│                      SOURCES                         │
│   PostgreSQL DB     CSV (SFTP)    Marketing REST API │
└────────┬─────────────────┬──────────────┬────────────┘
         │                 │              │
         ▼                 ▼              ▼
┌──────────────────────────────────────────────────────┐
│              EXTRACT  (Python scripts)               │
│  extract_postgres.py                                 │
│  extract_logistics_csv.py                            │
│  extract_marketing_api.py                            │
└──────────────────────────┬───────────────────────────┘
                           │
                           ▼
┌──────────────────────────────────────────────────────┐
│                    raw schema                        │
│        Unmodified copies of all source data          │
└──────────────────────────┬───────────────────────────┘
                           │
                           ▼
┌──────────────────────────────────────────────────────┐
│            TRANSFORM  (dbt staging models)           │
│        Clean, cast types, deduplicate, normalise     │
└──────────────────────────┬───────────────────────────┘
                           │
                           ▼
┌──────────────────────────────────────────────────────┐
│                  staging schema                      │
│   stg_orders  stg_customers  stg_products            │
│   stg_order_items  stg_shipments  stg_campaigns      │
└──────────────────────────┬───────────────────────────┘
                           │
                           ▼
┌──────────────────────────────────────────────────────┐
│             TRANSFORM  (dbt marts models)            │
│           Build star schema — facts + dimensions     │
└──────────────────────────┬───────────────────────────┘
                           │
                           ▼
┌──────────────────────────────────────────────────────┐
│                   marts schema                       │
│   fact_sales  dim_customers  dim_products  dim_date  │
└──────────────────────────┬───────────────────────────┘
                           │
                           ▼
┌──────────────────────────────────────────────────────┐
│            DATA QUALITY CHECKS  (Python)             │
│       5 automated checks → raw.pipeline_audit        │
└──────────────────────────┬───────────────────────────┘
                           │
                           ▼
┌──────────────────────────────────────────────────────┐
│            ORCHESTRATION  (Apache Airflow)           │
│   Scheduled daily at 06:30 UTC                       │
│   Parallel extraction → dbt → checks → notify        │
└──────────────────────────────────────────────────────┘
```

---

## Tech Stack

| Component        | Tool                    | Version |
|------------------|-------------------------|---------|
| Warehouse        | PostgreSQL              | 15      |
| Extraction       | Python                  | 3.10+   |
| Transformation   | dbt-core + dbt-postgres | 1.11    |
| Orchestration    | Apache Airflow          | 2.7     |
| Containerisation | Docker + Docker Compose | Latest  |

---

## Project Structure
```
de_project/
├── airflow/
│   ├── dags/
│   │   └── pipeline_dag.py           # Airflow DAG definition
│   ├── dbt/
│   │   └── profiles.yml              # dbt connection profile for Docker
│   └── requirements.txt              # Python packages for Airflow container
├── data/
│   └── csv/                          # Logistics CSV file drop location
├── dbt_project/
│   ├── models/
│   │   ├── staging/                  # Cleaned, typed views per source
│   │   └── marts/                    # Star schema: facts + dimensions
│   └── macros/
│       └── generate_schema_name.sql  # Custom schema naming macro
├── extract/
│   ├── extract_postgres.py           # Pulls from transactional source DB
│   ├── extract_logistics_csv.py      # Reads logistics CSV files
│   └── extract_marketing_api.py      # Calls marketing REST API
├── seed/
│   └── seed_postgres.py              # Populates source DB with sample data
├── data_quality_checks.py            # Automated pipeline health checks
├── setup_warehouse.py                # Creates warehouse schemas
├── health_check.py                   # Verifies all connections
├── Dockerfile                        # Custom Airflow image with dependencies
├── docker-compose.yml                # All services: DBs + Airflow
├── .env.example                      # Template for environment variables
├── DATA_DICTIONARY.md                # Column-level documentation
├── LINEAGE.md                        # Data lineage diagram
└── ADR.md                            # Architecture decision records
```

---

## Prerequisites

- Docker Desktop (running, with at least 4GB RAM allocated)
- Python 3.10+
- Git

---

## Setup Instructions

### 1. Clone the repository
```bash
git clone https://github.com/thenaijacarguy/de_project.git
cd de_project
```

### 2. Configure credentials
```bash
cp .env.example .env
# Open .env and update values if needed
# The defaults work out of the box for local development
```

### 3. Build and start all services
```bash
docker-compose up -d
```

This starts:

| Container          | Purpose                        | Port |
|--------------------|--------------------------------|------|
| source_db          | Transactional source database  | 5435 |
| warehouse_db       | Data warehouse                 | 5434 |
| airflow_db         | Airflow metadata database      | 5432 |
| airflow-scheduler  | DAG scheduler                  | —    |
| airflow-webserver  | Airflow UI                     | 8080 |

First run takes 3–5 minutes while Docker builds the custom Airflow image.

### 4. Seed the source database
```bash
python seed/seed_postgres.py
```

### 5. Set up the warehouse schemas
```bash
python setup_warehouse.py
```

### 6. Verify all connections
```bash
python health_check.py
```

You should see two green checkmarks. If not, check that Docker is running and all containers are healthy with `docker ps`.

### 7. Run the pipeline manually (local)
```bash
# Extract all three sources
python extract/extract_postgres.py
python extract/extract_logistics_csv.py
python extract/extract_marketing_api.py

# Transform with dbt
cd dbt_project && dbt run && dbt test && cd ..

# Run quality checks
python data_quality_checks.py
```

### 8. Run via Airflow

Open http://localhost:8080 and log in with `admin` / `admin`.

Find the `sales_pipeline` DAG, enable the toggle, then click the play button to trigger a manual run. Watch each task turn green in the Graph view.

---

## Data Sources

| Source             | Type       | Raw Tables                                           | Frequency          |
|--------------------|------------|------------------------------------------------------|--------------------|
| Transactional DB   | PostgreSQL | orders, order_items, customers, products             | Daily              |
| Logistics Provider | CSV (SFTP) | shipments                                            | Daily at 06:00 UTC |
| Marketing Platform | REST API   | campaigns                                            | Daily              |

---

## Warehouse Schema

### raw
Unmodified copies of source data. All columns stored as TEXT. Never queried directly by analysts. Overwritten on each pipeline run (full refresh) or appended idempotently (CSV and API sources).

### staging
Cleaned, typed, deduplicated views built by dbt. One view per raw table. No joins across sources. Materialised as views so they always reflect the latest raw data without storing a copy.

### marts
Star schema tables materialised as physical tables for fast query performance.

| Table         | Type      | Description                                         |
|---------------|-----------|-----------------------------------------------------|
| fact_sales    | Fact      | One row per order line item — the central table     |
| dim_customers | Dimension | Customer name, email, region, days since signup     |
| dim_products  | Dimension | Product name, category, cost price                  |
| dim_date      | Dimension | Calendar attributes for every day from 2023–2025    |

---

## Data Quality Checks

Five automated checks run after every pipeline execution. Results are logged to `raw.pipeline_audit` with timestamps so you can track pipeline health over time.

| Check                         | What It Catches                                    |
|-------------------------------|----------------------------------------------------|
| row_count_validation          | fact_sales has fewer rows than expected            |
| null_rate_{column}            | Critical columns contain NULL values               |
| referential_integrity_customers | Orphaned customer_id foreign keys in fact_sales  |
| referential_integrity_products  | Orphaned product_id foreign keys in fact_sales   |
| data_freshness                | Most recent order date is unexpectedly old         |
| revenue_sanity                | Line revenue outliers more than 10x the average    |

To view the audit log:
```sql
SELECT check_name, status, details, checked_at
FROM raw.pipeline_audit
ORDER BY checked_at DESC;
```

---

## Airflow DAG

The `sales_pipeline` DAG runs daily at 06:30 UTC with the following task graph:
```
extract_postgres ──┐
extract_csv     ───┼──► dbt_run ──► dbt_test ──► quality_checks ──► branch ──► notify_success
extract_api     ──┘                                                         └──► notify_failure
```

Key settings:
- **Schedule:** `30 6 * * *` (06:30 UTC daily)
- **Retries:** 3 attempts per task, 5 minute delay between retries
- **Parallelism:** The three extraction tasks run simultaneously
- **Branching:** The final notification task routes to success or failure based on quality check results

---

## Adding a New Data Source

1. Write an extraction script in `extract/` following the same pattern as existing scripts — load credentials from `.env`, write raw data to the warehouse, log row counts, handle errors
2. Create a staging model in `dbt_project/models/staging/`
3. Add column descriptions and tests to `schema.yml`
4. Join the new staging model into `fact_sales.sql` if relevant
5. Add a new task to the DAG in `airflow/dags/pipeline_dag.py`
6. Add the new package to `Dockerfile` if needed, then rebuild with `docker-compose build --no-cache`

---

## Known Limitations

- **Seed data is frozen in 2024.** The sample data generated by `seed_postgres.py` has order dates up to December 2024. The data freshness quality check threshold has been relaxed to 500 days to accommodate this. In a production pipeline with live data the threshold would be 1–2 days.
- **Full refresh only.** The extraction scripts reload all data on every run. For tables with millions of rows this would be too slow — incremental loading with CDC (Change Data Capture) would be required at scale.
- **Single-node Airflow.** We use the LocalExecutor which runs tasks on a single machine. A production deployment would use the CeleryExecutor or KubernetesExecutor to distribute tasks across multiple workers.
- **Mock marketing API.** The marketing API extraction uses JSONPlaceholder as a mock endpoint. The response structure differs from a real marketing platform — the staging model would need updating to match a real API's schema.
- **No SFTP.** The logistics CSV extraction reads from a local folder rather than an actual SFTP server. In production you would use the `paramiko` library to connect to the SFTP server and download files automatically.

---

## Troubleshooting

**Tasks stuck yellow in Airflow for more than 2 minutes**
```bash
docker ps                                              # check all containers are running
docker-compose logs airflow-scheduler | tail -30       # check scheduler logs
```

**Connection refused errors in extraction scripts**

Scripts running on your Mac use `localhost` with external ports (5435, 5434). Scripts running inside Airflow containers use container names (`source_db`, `warehouse_db`) on port 5432. If you see connection errors inside Airflow, check the environment variable overrides in `docker-compose.yml`.

**dbt writes to wrong schema (e.g. public_marts instead of marts)**

Ensure `dbt_project/macros/generate_schema_name.sql` exists and that `dbt_project.yml` has `+schema: staging` and `+schema: marts` under the respective model groups.

**pip install permission denied inside Airflow container**

The Airflow image blocks runtime pip installs. Add the package to the `Dockerfile` RUN command and rebuild:
```bash
docker-compose down
docker-compose build --no-cache
docker-compose up -d
```

**GitHub push rejected**

GitHub does not accept account passwords for Git operations. Use a Personal Access Token (PAT) with `repo` scope. Generate one at GitHub → Settings → Developer Settings → Personal Access Tokens.

---

## Key Gotchas

These are things that caused real debugging time during this project and are worth knowing upfront:

- **Docker networking vs localhost.** Inside Docker, containers reach each other by container name on port 5432, not by localhost on your exposed ports. The same script needs different connection settings depending on whether it runs on your Mac or inside Airflow.
- **dbt schema prefixing.** By default dbt prefixes your custom schema names with the default schema, producing names like `public_marts` instead of `marts`. The fix is a custom `generate_schema_name` macro and explicit `+schema` config in `dbt_project.yml`.
- **Airflow pip restrictions.** The official Airflow Docker image intentionally blocks pip at runtime. All Python dependencies must be baked into a custom Docker image at build time via a `Dockerfile`.
- **dbt init creates a nested project.** Running `dbt init` inside an existing project folder creates a duplicate nested subfolder. Delete it with `rm -rf dbt_project/de_project`.
- **provide_context deprecation.** In Airflow 2.0+ `provide_context=True` is no longer needed on PythonOperator but causes deprecation warnings if included. It is harmless but can be removed.

---

## Version History

| Version | Date       | Description                          |
|---------|------------|--------------------------------------|
| 1.0.0   | 2026-02-26 | Initial complete pipeline            |

---

## Contributors

| Name          | Role               | GitHub                                          |
|---------------|--------------------|-------------------------------------------------|
| Gabriel James | Analytics Engineer | [@thenaijacarguy](https://github.com/thenaijacarguy) |

---

## Licence

This project is for educational and portfolio purposes. Feel free to fork it, adapt it, and use it as a reference for your own projects.