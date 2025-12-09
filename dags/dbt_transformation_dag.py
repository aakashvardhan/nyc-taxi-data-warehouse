"""
Airflow DAG for dbt transformation pipeline
Runs dbt models to transform RAW data into analytics-ready tables in ANALYTICS schema
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.models import Variable

SNOWFLAKE_CONN_ID = "snowflake_catfish"
DBT_PROJECT_DIR = "/opt/airflow/dbt"

default_args = {
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

def ensure_schemas(**context):
    """Create ANALYTICS and SNAPSHOTS schemas if they don't exist"""
    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    
    schemas = ["ANALYTICS", "SNAPSHOTS"]
    
    with hook.get_conn() as conn, conn.cursor() as cur:
        try:
            conn.autocommit(False)
            for schema in schemas:
                cur.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')
                logging.info(f"Ensured {schema} schema exists")
            conn.commit()
            return f"Schemas ready: {', '.join(schemas)}"
        except Exception as e:
            logging.exception("Error ensuring schemas")
            try:
                conn.rollback()
            except Exception:
                pass
            raise e


with DAG(
    dag_id="dbt_transformation_pipeline",
    description="Transform RAW data using dbt models into ANALYTICS schema",
    schedule="0 2 * * *",  # Daily at 2 AM UTC
    start_date=datetime(2025, 11, 1, tzinfo=timezone.utc),
    catchup=False,
    default_args=default_args,
    tags=["dbt", "transformation", "analytics"],
) as dag:

    # Task 0: Wait for weather ETL to complete (checks last hour's run)
    # Uses execution_delta to look back for the previous weather run at 1 AM UTC
    t_wait_weather = ExternalTaskSensor(
        task_id="wait_for_weather_etl",
        external_dag_id="nyc_weather_realtime_etl",
        external_task_id="fetch_and_load_weather",
        execution_delta=timedelta(hours=1),  # dbt runs at 2AM, weather runs hourly
        mode="reschedule",  # Don't block worker slot while waiting
        timeout=3600,  # 1 hour timeout
        poke_interval=300,  # Check every 5 minutes
        soft_fail=True,  # Don't fail pipeline if weather ETL missed
    )

    # Task 1: Ensure ANALYTICS and SNAPSHOTS schemas exist
    t_ensure_schema = PythonOperator(
        task_id="ensure_schemas",
        python_callable=ensure_schemas,
    )

    # Task 2: Install dbt dependencies (dbt_utils)
    t_dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt deps --profiles-dir {DBT_PROJECT_DIR}",
    )

    # Task 3: Check source freshness (runs early, soft_fail to avoid blocking)
    t_dbt_source_freshness = BashOperator(
        task_id="dbt_source_freshness",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt source freshness --profiles-dir {DBT_PROJECT_DIR} || true",
    )

    # Task 4: Run dbt models (all layers: staging -> intermediate -> marts)
    t_dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt run --profiles-dir {DBT_PROJECT_DIR}",
    )

    # Task 5: Run dbt snapshots (SCD Type 2 for weather and metrics history)
    # Snapshots run AFTER models because snp_daily_metrics depends on mart_daily_metrics
    t_dbt_snapshot = BashOperator(
        task_id="dbt_snapshot",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt snapshot --profiles-dir {DBT_PROJECT_DIR}",
    )

    # Task 6: Run dbt tests to validate data quality
    t_dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt test --profiles-dir {DBT_PROJECT_DIR}",
    )

    # Task 7: Generate dbt documentation
    t_dbt_docs_generate = BashOperator(
        task_id="dbt_docs_generate",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt docs generate --profiles-dir {DBT_PROJECT_DIR}",
    )

    # Define task dependencies
    # wait_weather -> ensure_schema -> deps -> freshness -> run -> snapshot -> test -> docs
    # Note: Snapshots run AFTER models because snp_daily_metrics refs mart_daily_metrics
    (
        t_wait_weather
        >> t_ensure_schema
        >> t_dbt_deps
        >> t_dbt_source_freshness
        >> t_dbt_run
        >> t_dbt_snapshot
        >> t_dbt_test
        >> t_dbt_docs_generate
    )
