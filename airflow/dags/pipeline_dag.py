from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime, timedelta
import subprocess
import sys
import os

# ── Default arguments ──────────────────────────────────────────────────────
# These settings apply to every task in the DAG unless overridden.
default_args = {
    'owner': 'data_engineering',

    # If a task fails, retry it up to 3 times before marking it as failed.
    # This handles transient issues like a momentary network blip.
    'retries': 3,

    # Wait 5 minutes between retries. This gives temporary issues
    # (overloaded API, brief DB unavailability) time to resolve.
    'retry_delay': timedelta(minutes=5),

    # Email on failure — in production you'd configure SMTP in Airflow.
    # For now this just sets the intent; we'll mock the notification.
    'email_on_failure': False,
    'email_on_retry': False,
}

# ── Helper: run a Python script as a subprocess ────────────────────────────
def run_script(script_path):
    """
    Runs a Python script as a subprocess from inside the Airflow container.

    We use subprocess rather than importing and calling the functions directly
    because each script manages its own database connections. Running them
    as subprocesses keeps them fully isolated — a crash in one script
    won't affect the Airflow worker process itself.

    script_path = path relative to /opt/airflow/project (the mounted project root)
    """
    full_path = f"/opt/airflow/project/{script_path}"

    # Set DBT_PROFILES_DIR so dbt finds the profiles.yml we copied into
    # the project. Without this, dbt would look in ~/.dbt/ which doesn't
    # exist inside the container.
    env = os.environ.copy()
    env['DBT_PROFILES_DIR'] = '/opt/airflow/project/airflow/dbt'

    result = subprocess.run(
        [sys.executable, full_path],   # sys.executable = the Python interpreter
        capture_output=True,           # capture stdout and stderr
        text=True,                     # return output as strings, not bytes
        env=env
    )

    # Print output so it appears in Airflow's task logs
    if result.stdout:
        print(result.stdout)
    if result.stderr:
        print(result.stderr)

    # If the script exited with a non-zero code (failure), raise an
    # exception. This tells Airflow the task failed, which triggers
    # the retry logic and eventually marks the task red in the UI.
    if result.returncode != 0:
        raise Exception(f"Script {script_path} failed with exit code {result.returncode}")


def run_dbt(command):
    """
    Runs a dbt command (e.g. 'run' or 'test') inside the container.

    Similar to run_script but uses the dbt CLI instead of Python.
    """
    env = os.environ.copy()
    env['DBT_PROFILES_DIR'] = '/opt/airflow/project/airflow/dbt'

    result = subprocess.run(
        ['dbt', command,
         '--project-dir', '/opt/airflow/project/dbt_project',
         '--profiles-dir', '/opt/airflow/project/airflow/dbt'],
        capture_output=True,
        text=True,
        env=env
    )

    if result.stdout:
        print(result.stdout)
    if result.stderr:
        print(result.stderr)

    if result.returncode != 0:
        raise Exception(f"dbt {command} failed with exit code {result.returncode}")


# ── Individual task functions ──────────────────────────────────────────────
# Each function below becomes one task in the DAG.
# Airflow calls these functions when it's time to run the task.

def extract_postgres_task():
    run_script("extract/extract_postgres.py")

def extract_csv_task():
    run_script("extract/extract_logistics_csv.py")

def extract_api_task():
    run_script("extract/extract_marketing_api.py")

def dbt_run_task():
    run_dbt("run")

def dbt_test_task():
    run_dbt("test")

def quality_checks_task(**context):
    """
    Runs data quality checks and pushes the result to XCom.

    XCom (Cross-Communication) is Airflow's way of passing small pieces
    of data between tasks. Here we push a True/False value so the next
    task (the branch) can decide whether to notify success or failure.

    context is Airflow's task context dictionary — it contains metadata
    about the current run. We use context['ti'] to access the TaskInstance,
    which is what lets us push/pull XCom values.
    """
    # We run the quality checks script and capture whether it passed
    full_path = "/opt/airflow/project/data_quality_checks.py"
    env = os.environ.copy()

    result = subprocess.run(
        [sys.executable, full_path],
        capture_output=True,
        text=True,
        env=env
    )

    if result.stdout:
        print(result.stdout)
    if result.stderr:
        print(result.stderr)

    # Push the result (True/False) to XCom under the key 'checks_passed'
    # The branch task will pull this value to decide what to do next
    checks_passed = result.returncode == 0
    context['ti'].xcom_push(key='checks_passed', value=checks_passed)

    # We don't raise an exception here even if checks failed —
    # we want the pipeline to continue to the notification step
    # so we can send a proper failure alert rather than just erroring out


def branch_task(**context):
    """
    Decides which notification task to run based on quality check results.

    This is a BranchPythonOperator task — it must return the task_id
    of the next task to run. Airflow will skip all other downstream tasks.

    We pull the XCom value that quality_checks_task pushed earlier.
    """
    checks_passed = context['ti'].xcom_pull(
        task_ids='quality_checks',
        key='checks_passed'
    )

    if checks_passed:
        return 'notify_success'   # return the task_id to run next
    else:
        return 'notify_failure'


def notify_success_task():
    """
    In production this would send a Slack message or email.
    For now it just prints — you'd replace this with a real notification.
    """
    print("✅ Pipeline completed successfully! All quality checks passed.")
    print("   Data is fresh and ready for analysts.")


def notify_failure_task():
    """
    In production this would page the on-call engineer.
    """
    print("❌ Pipeline completed but quality checks FAILED!")
    print("   Check raw.pipeline_audit for details.")
    print("   Analysts have been warned not to use today's data.")


# ── Define the DAG ─────────────────────────────────────────────────────────
with DAG(
    dag_id='sales_pipeline',           # unique name shown in the Airflow UI
    default_args=default_args,
    description='Daily sales ETL pipeline: extract → transform → quality checks',

    # Cron expression: run at 06:30 UTC every day.
    # Cron format: minute hour day month weekday
    # '30 6 * * *' = at 06:30, every day, every month, any weekday
    schedule_interval='30 6 * * *',

    start_date=datetime(2024, 1, 1),   # when the DAG became active
    catchup=False,                     # don't backfill all past runs since start_date
    tags=['sales', 'etl'],
) as dag:

    # ── Define tasks ───────────────────────────────────────────────────────
    # Each PythonOperator wraps one Python function as an Airflow task.
    # task_id is the name shown in the UI and used in dependencies.

    extract_postgres = PythonOperator(
        task_id='extract_postgres',
        python_callable=extract_postgres_task,
    )

    extract_csv = PythonOperator(
        task_id='extract_csv',
        python_callable=extract_csv_task,
    )

    extract_api = PythonOperator(
        task_id='extract_api',
        python_callable=extract_api_task,
    )

    dbt_run = PythonOperator(
        task_id='dbt_run',
        python_callable=dbt_run_task,
    )

    dbt_test = PythonOperator(
        task_id='dbt_test',
        python_callable=dbt_test_task,
    )

    quality_checks = PythonOperator(
        task_id='quality_checks',
        python_callable=quality_checks_task,
        provide_context=True,          # gives the function access to **context
    )

    # BranchPythonOperator: runs the function and uses its return value
    # (a task_id string) to decide which downstream task to execute
    branch = BranchPythonOperator(
        task_id='branch',
        python_callable=branch_task,
        provide_context=True,
    )

    notify_success = PythonOperator(
        task_id='notify_success',
        python_callable=notify_success_task,
        # TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS means: run this task
        # only if the branch task chose me and no upstream tasks failed.
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    notify_failure = PythonOperator(
        task_id='notify_failure',
        python_callable=notify_failure_task,
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    # ── Define dependencies (the shape of the DAG) ─────────────────────────
    # The >> operator means "this task must finish before that task starts".
    # Reading it like a sentence: "extract_postgres feeds into dbt_run"

    # The three extractions run in parallel (no dependency between them)
    # then dbt_run waits for ALL THREE to complete before starting.
    [extract_postgres, extract_csv, extract_api] >> dbt_run

    # dbt_run must finish before dbt_test starts
    dbt_run >> dbt_test

    # dbt_test must pass before quality checks run
    dbt_test >> quality_checks

    # quality_checks feeds into the branch which picks success or failure
    quality_checks >> branch >> [notify_success, notify_failure]