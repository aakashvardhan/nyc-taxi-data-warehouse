from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime, timedelta
import pandas as pd
import joblib
import os

SNOWFLAKE_CONN_ID = "SNOWFLAKE_CONN_ID"
MODEL_PATH = "/opt/airflow/models/fare_model.pkl"

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

def generate_forecast(**context):

    # -----------------------------
    # 1. Load trained model
    # -----------------------------
    model = joblib.load(MODEL_PATH)
    print("Loaded trained model.")

    # -----------------------------
    # 2. Get latest historical features
    # -----------------------------
    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    engine = hook.get_sqlalchemy_engine()

    latest_query = """
        SELECT *
        FROM USER_DB_CAT.ANALYTICS.FARE_DAILY_FEATURES
        ORDER BY TRIP_DATE DESC
        LIMIT 1;
    """
    last_row = pd.read_sql(latest_query, engine)
    last_row.columns = last_row.columns.str.upper()

    latest_trip_date = last_row["TRIP_DATE"].iloc[0]
    print("Latest historical date:", latest_trip_date)

    last_trip_count     = float(last_row["TRIP_COUNT"].iloc[0])
    last_avg_distance   = float(last_row["AVG_DISTANCE"].iloc[0])
    last_temp           = float(last_row["AVG_DAILY_TEMPERATURE"].iloc[0])
    last_humidity       = float(last_row["AVG_DAILY_HUMIDITY"].iloc[0])
    last_precip         = float(last_row["IS_PRECIP_DAY"].iloc[0])

    last_avg_fare       = float(last_row["AVG_FARE"].iloc[0])
    last_lag1           = float(last_row["AVG_FARE_LAG1"].iloc[0])
    last_lag7           = float(last_row["AVG_FARE_LAG7"].iloc[0])
    last_ma7            = float(last_row["AVG_FARE_MA7"].iloc[0])

    # -----------------------------
    # 3. Build next 7 days dataframe
    # -----------------------------
    future_rows = []
    today = latest_trip_date

    for i in range(1, 8):
        forecast_date = today + timedelta(days=i)
        dow = forecast_date.weekday()

        future_rows.append({
            "TRIP_DATE": forecast_date,
            "TRIP_COUNT": last_trip_count,
            "AVG_DISTANCE": last_avg_distance,
            "WEEKEND_TRIP_COUNT": 1 if dow in (5, 6) else 0,
            "WEEKDAY_TRIP_COUNT": 1 if dow not in (5, 6) else 0,
            "AVG_DAILY_TEMPERATURE": last_temp,
            "AVG_DAILY_HUMIDITY": last_humidity,
            "IS_PRECIP_DAY": last_precip,
            "AVG_FARE_LAG1": last_avg_fare,
            "AVG_FARE_LAG7": last_lag7,
            "AVG_FARE_MA7": last_ma7,
        })

        last_lag7 = last_lag1
        last_lag1 = last_avg_fare

    df_future = pd.DataFrame(future_rows)

    feature_cols = [
        "TRIP_COUNT",
        "AVG_DISTANCE",
        "WEEKEND_TRIP_COUNT",
        "WEEKDAY_TRIP_COUNT",
        "AVG_DAILY_TEMPERATURE",
        "AVG_DAILY_HUMIDITY",
        "IS_PRECIP_DAY",
        "AVG_FARE_LAG1",
        "AVG_FARE_LAG7",
        "AVG_FARE_MA7",
    ]

    X_future = df_future[feature_cols].fillna(0)

    # -----------------------------
    # 4. Predict
    # -----------------------------
    df_future["PREDICTED_AVG_FARE"] = model.predict(X_future)

    print("Forecast created:")
    print(df_future[["TRIP_DATE", "PREDICTED_AVG_FARE"]])

    # -----------------------------
    # 5. Prepare insert DF
    # -----------------------------
    df_future["FORECAST_DATE"] = df_future["TRIP_DATE"].apply(
        lambda x: x.date() if hasattr(x, "date") else x
    )

    df_insert = df_future[["FORECAST_DATE", "PREDICTED_AVG_FARE"]].copy()
    df_insert["MODEL_VERSION"] = "rf_v1"

    from sqlalchemy import text
    insert_sql = text("""
        INSERT INTO USER_DB_CAT.ANALYTICS.FARE_DAILY_FORECAST
        (FORECAST_DATE, PREDICTED_AVG_FARE, MODEL_VERSION)
        VALUES (:date, :fare, :version)
    """)

    # -------------------------------------------------------
    # 6. FIX: TRUNCATE TABLE BEFORE INSERT TO AVOID DUPLICATES
    # -------------------------------------------------------
    with hook.get_conn() as conn, conn.cursor() as cur:
        cur.execute("TRUNCATE TABLE USER_DB_CAT.ANALYTICS.FARE_DAILY_FORECAST")

    # -----------------------------
    # 7. Insert forecast rows
    # -----------------------------
    with engine.begin() as conn:
        for _, row in df_insert.iterrows():
            conn.execute(insert_sql, {
                "date": row["FORECAST_DATE"],
                "fare": float(row["PREDICTED_AVG_FARE"]),
                "version": row["MODEL_VERSION"]
            })

    print("Forecast saved to Snowflake.")


# -----------------------------
# DAG DEFINITION
# -----------------------------
with DAG(
    dag_id="fare_forecasting_dag",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["ml", "forecast"],
) as dag:

    forecast_task = PythonOperator(
        task_id="generate_forecast",
        python_callable=generate_forecast,
    )
