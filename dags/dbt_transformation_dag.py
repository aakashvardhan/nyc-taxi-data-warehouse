"""
Airflow DAG for dbt transformation pipeline
Runs dbt models to transform RAW data into analytics-ready tables in ANALYTICS schema
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.models import Variable

SNOWFLAKE_CONN_ID = "snowflake_catfish"
DBT_PROJECT_DIR = "/opt/airflow/dbt"

default_args = {
    "depends_on_past": False,
    "retries": 1,
}

def ensure_analytics_schema(**context):
    """Create ANALYTICS schema if it doesn't exist"""
    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    
    with hook.get_conn() as conn, conn.cursor() as cur:
        try:
            conn.autocommit(False)
            cur.execute('CREATE SCHEMA IF NOT EXISTS "ANALYTICS"')
            conn.commit()
            logging.info("Ensured ANALYTICS schema exists")
            return "ANALYTICS schema ready"
        except Exception as e:
            logging.exception("Error ensuring ANALYTICS schema")
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

    # Task 1: Ensure ANALYTICS schema exists
    t_ensure_schema = PythonOperator(
        task_id="ensure_analytics_schema",
        python_callable=ensure_analytics_schema,
    )

    # Task 2: Install dbt dependencies (dbt_utils)
    t_dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt deps --profiles-dir {DBT_PROJECT_DIR}",
    )

    # Task 3: Run dbt models (all layers: staging -> intermediate -> marts)
    t_dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt run --profiles-dir {DBT_PROJECT_DIR}",
    )

    # Task 4: Run dbt tests to validate data quality
    t_dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt test --profiles-dir {DBT_PROJECT_DIR}",
    )

    # Task 5: Check source freshness
    t_dbt_source_freshness = BashOperator(
        task_id="dbt_source_freshness",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt source freshness --profiles-dir {DBT_PROJECT_DIR}",
    )

    # Task 6: Generate dbt documentation (optional)
    t_dbt_docs_generate = BashOperator(
        task_id="dbt_docs_generate",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt docs generate --profiles-dir {DBT_PROJECT_DIR}",
    )

    # Define task dependencies
    t_ensure_schema >> t_dbt_deps >> t_dbt_source_freshness >> t_dbt_run >> t_dbt_test >> t_dbt_docs_generate
