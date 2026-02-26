FROM apache/airflow:2.7.0

USER airflow

RUN pip install --no-cache-dir \
    psycopg2-binary \
    pandas \
    requests \
    python-dotenv \
    dbt-core \
    dbt-postgres