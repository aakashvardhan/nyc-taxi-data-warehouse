# -----------------------------
# FIXED ZONE DEMAND CTAS DAG
# WORKS WITH UPPERCASE SNOWFLAKE COLUMNS
# -----------------------------
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime

SNOWFLAKE_CONN_ID = "SNOWFLAKE_CONN_ID"

def build_zone_demand_ctas(**context):

    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    engine = hook.get_sqlalchemy_engine()

    # FIX: Fully qualified AND uppercase-safe SQL
    trips_sql = """
        SELECT 
            PICKUP_DATETIME,
            PICKUP_ZONE_ID,
            TRIP_DISTANCE,
            TOTAL_AMOUNT
        FROM USER_DB_CAT.RAW.NYC_TAXI_TRIPS
    """

    trips = pd.read_sql(trips_sql, engine)

    # FIX: Normalize all columns to uppercase
    trips.columns = trips.columns.str.upper()

    # FIX: Convert pickup datetime to date
    trips["TRIP_DATE"] = pd.to_datetime(trips["PICKUP_DATETIME"]).dt.date

    # FIX: Correct grouping using PICKUP_ZONE_ID
    zone_df = (
        trips.groupby(["PICKUP_ZONE_ID", "TRIP_DATE"])
        .agg(
            TRIP_COUNT=("PICKUP_ZONE_ID", "count"),
            AVG_DISTANCE=("TRIP_DISTANCE", "mean"),
            AVG_FARE=("TOTAL_AMOUNT", "mean")
        )
        .reset_index()
    )

    zones = pd.read_csv("https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv")
    zones.columns = zones.columns.str.upper()

    # FIX: Join on correct key
    zone_df = zone_df.merge(
        zones,
        left_on="PICKUP_ZONE_ID",
        right_on="LOCATIONID",
        how="left"
    )

    # FIX: rename for clarity
    zone_df.rename(columns={"PICKUP_ZONE_ID": "ZONE_ID"}, inplace=True)

    # UPLOAD TO TMP TABLE IN ANALYTICS SCHEMA
    zone_df.to_sql(
        "ZONE_DEMAND_TMP",
        engine,
        index=False,
        if_exists="replace",
        schema="ANALYTICS"
    )

    # CTAS → final table
    ctas_sql = """
        CREATE OR REPLACE TABLE USER_DB_CAT.ANALYTICS.ZONE_DEMAND AS
        SELECT 
            CAST(TRIP_DATE AS DATE) AS TRIP_DATE,
            ZONE_ID,
            BOROUGH,
            ZONE,
            TRIP_COUNT,
            AVG_DISTANCE,
            AVG_FARE
        FROM USER_DB_CAT.ANALYTICS.ZONE_DEMAND_TMP
    """

    with engine.begin() as conn:
        conn.execute(ctas_sql)


with DAG(
    dag_id="zone_demand_dag",
    start_date=datetime(2025,1,1),
    schedule="@daily",
    catchup=False,
) as dag_zone:

    PythonOperator(
        task_id="zone_demand_ctas",
        python_callable=build_zone_demand_ctas
    )
