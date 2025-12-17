# -----------------------------
# FIXED WEATHER FORECAST CTAS FEATURE
# (writes directly into USER_DB_CAT.ANALYTICS schema)
# -----------------------------
import requests
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime, timedelta

SNOWFLAKE_CONN_ID = "SNOWFLAKE_CONN_ID"

def build_weather_forecast_ctas(**context):

    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": 40.7128,
        "longitude": -74.0060,
        "hourly": "temperature_2m,relativehumidity_2m,precipitation",
        "forecast_days": 7,
        "timezone": "America/New_York"
    }

    data = requests.get(url, params=params).json()
    hourly = pd.DataFrame(data["hourly"])
    hourly["time"] = pd.to_datetime(hourly["time"])
    hourly["DATE"] = hourly["time"].dt.date

    df = hourly.groupby("DATE").agg(
        AVG_TEMP=("temperature_2m","mean"),
        AVG_HUMIDITY=("relativehumidity_2m","mean"),
        IS_PRECIP_DAY=("precipitation", lambda x: 1 if x.sum()>0 else 0)
    ).reset_index()

    df.columns = ["FORECAST_DATE","AVG_TEMP","AVG_HUMIDITY","IS_PRECIP_DAY"]

    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    engine = hook.get_sqlalchemy_engine()
    with hook.get_conn() as conn, conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS USER_DB_CAT.ANALYTICS.WEATHER_FORECAST_DAILY_TMP")

    # write to fully-qualified table
        df.to_sql(
            "WEATHER_FORECAST_DAILY_TMP",
            engine,
            index=False,
            if_exists="append",   # <-- IMPORTANT: do NOT use replace!
            schema="ANALYTICS"
        )

    ctas_sql = """
    CREATE OR REPLACE TABLE USER_DB_CAT.ANALYTICS.WEATHER_FORECAST_DAILY AS
    SELECT 
        CAST(FORECAST_DATE AS DATE) AS FORECAST_DATE,
        AVG_TEMP,
        AVG_HUMIDITY,
        IS_PRECIP_DAY
    FROM USER_DB_CAT.ANALYTICS.WEATHER_FORECAST_DAILY_TMP
    """

    with engine.begin() as conn:
        conn.execute(ctas_sql)


with DAG(
    dag_id="weather_forecast_dag",
    start_date=datetime(2025,1,1),
    schedule="@daily",
    catchup=False,
) as dag_weather:

    PythonOperator(
        task_id="weather_forecast_ctas",
        python_callable=build_weather_forecast_ctas
    )
