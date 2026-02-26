# Sales Data Pipeline

An end-to-end data engineering pipeline that consolidates sales data
from three sources into a analytics-ready data warehouse.

## Architecture
```
3 Sources → Extract (Python) → raw schema
                                    ↓
                            dbt staging models
                                    ↓
                            dbt marts (star schema)
                                    ↓
                          Data quality checks
                                    ↓
                          Airflow orchestration
```

## Tech Stack

| Component      | Tool                        |
|----------------|-----------------------------|
| Warehouse      | PostgreSQL 15               |
| Extraction     | Python 3.10+                |
| Transformation | dbt-core + dbt-postgres     |
| Orchestration  | Apache Airflow 2.7          |
| Containerisation | Docker + Docker Compose   |

## Project Structure
```
de_project/
├── airflow/
│   ├── dags/
│   │   └── pipeline_dag.py      # Airflow DAG definition
│   ├── dbt/
│   │   └── profiles.yml         # dbt connection profile for Docker
│   └── requirements.txt         # Python packages for Airflow container
├── data/
│   └── csv/                     # Logistics CSV file drop location
├── dbt_project/
│   ├── models/
│   │   ├── staging/             # Cleaned, typed views per source
│   │   └── marts/               # Star schema: facts + dimensions
│   └── macros/
│       └── generate_schema_name.sql  # Custom schema naming macro
├── extract/
│   ├── extract_postgres.py      # Pulls from transactional source DB
│   ├── extract_logistics_csv.py # Reads logistics CSV files
│   └── extract_marketing_api.py # Calls marketing REST API
├── seed/
│   └── seed_postgres.py         # Populates source DB with sample data
├── data_quality_checks.py       # Automated pipeline health checks
├── setup_warehouse.py           # Creates warehouse schemas
├── health_check.py              # Verifies all connections
├── docker-compose.yml           # All services: DBs + Airflow
└── Dockerfile                   # Custom Airflow image with dependencies
```

## Prerequisites

- Docker Desktop (running)
- Python 3.10+
- Git

## Setup Instructions

### 1. Clone the repository
```bash
git clone https://github.com/thenaijacarguy/de_project.git
cd de_project
```

### 2. Configure credentials
```bash
cp .env.example .env
# Open .env and fill in any values you want to change
# The defaults work out of the box for local development
```

### 3. Start all services
```bash
docker-compose up -d
```

This starts:
- `source_db` on port 5435 — transactional source database
- `warehouse_db` on port 5434 — data warehouse
- `airflow_db` on port 5432 — Airflow metadata database
- `airflow-scheduler` — DAG scheduler
- `airflow-webserver` on port 8080 — Airflow UI

First run takes 3–5 minutes while Docker pulls and builds images.

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

You should see two green checkmarks. If not, check that Docker is
running and all containers are up with `docker ps`.

### 7. Run the pipeline manually (local)
```bash
# Extract
python extract/extract_postgres.py
python extract/extract_logistics_csv.py
python extract/extract_marketing_api.py

# Transform
cd dbt_project && dbt run && dbt test && cd ..

# Quality checks
python data_quality_checks.py
```

### 8. Run via Airflow

Open http://localhost:8080 and log in with admin / admin.

Find the `sales_pipeline` DAG, enable it with the toggle, then
click the play button to trigger a manual run.

## Data Sources

| Source | Type | Tables | Update Frequency |
|--------|------|--------|-----------------|
| Transactional DB | PostgreSQL | orders, order_items, customers, products | Daily |
| Logistics Provider | CSV (SFTP) | shipments | Daily at 06:00 UTC |
| Marketing Platform | REST API | campaigns | Daily |

## Warehouse Schema

### raw
Unmodified copies of source data. One table per source extract.
Never queried directly by analysts.

### staging
Cleaned, typed, deduplicated views built by dbt.
One view per raw table. No joins across sources.

### marts
Star schema tables ready for analytics.
- `fact_sales` — one row per order item sold
- `dim_customers` — customer attributes
- `dim_products` — product attributes
- `dim_date` — date dimension for time-based analysis

## Data Quality Checks

Five automated checks run after every pipeline execution and log
results to `raw.pipeline_audit`:

| Check | What It Catches |
|-------|----------------|
| row_count_validation | Silent extraction failures |
| null_rate checks | Missing values in critical columns |
| referential_integrity | Orphaned foreign keys |
| data_freshness | Stale or missing data |
| revenue_sanity | Pricing errors and outliers |

## Adding a New Data Source

1. Write an extraction script in `extract/` following the same
   pattern as the existing scripts (load credentials from .env,
   write raw data to warehouse, log row counts)
2. Create a staging model in `dbt_project/models/staging/`
3. Add the new columns to `fact_sales.sql` if needed
4. Add schema tests to `schema.yml`
5. Add a task to the Airflow DAG in `airflow/dags/pipeline_dag.py`

## Troubleshooting

**Tasks stuck yellow in Airflow**
Check that all containers are running: `docker ps`
Check scheduler logs: `docker-compose logs airflow-scheduler | tail -30`

**Connection refused errors**
Scripts running locally use localhost with external ports (5433, 5434).
Scripts running inside Airflow use container names (source_db, warehouse_db) on port 5432.
Check your .env and docker-compose.yml environment overrides.

**dbt schema errors**
Ensure `~/.dbt/profiles.yml` has `schema: public` and the
`generate_schema_name.sql` macro exists in `dbt_project/macros/`.

**Permission denied on pip**
The Airflow image blocks runtime pip installs. Add packages to
the `Dockerfile` and rebuild with `docker-compose build --no-cache`.
