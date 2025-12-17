from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import joblib
from sklearn.metrics import mean_absolute_error

SNOWFLAKE_CONN_ID = "SNOWFLAKE_CONN_ID"
MODEL_PATH = "/opt/airflow/models/fare_model.pkl"

def build_forecast_eval_ctas(**context):

    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    engine = hook.get_sqlalchemy_engine()

    # ---------------------------------------------------
    # Load actuals
    # ---------------------------------------------------
    actual = pd.read_sql("""
        SELECT TRIP_DATE, AVG_FARE,
               TRIP_COUNT, AVG_DISTANCE,
               WEEKEND_TRIP_COUNT, WEEKDAY_TRIP_COUNT,
               AVG_DAILY_TEMPERATURE, AVG_DAILY_HUMIDITY,
               IS_PRECIP_DAY,
               AVG_FARE_LAG1, AVG_FARE_LAG7, AVG_FARE_MA7
        FROM USER_DB_CAT.ANALYTICS.FARE_DAILY_FEATURES
        ORDER BY TRIP_DATE
    """, engine)

    actual.columns = actual.columns.str.upper()
    actual["TRIP_DATE"] = pd.to_datetime(actual["TRIP_DATE"]).dt.date

    # ---------------------------------------------------
    # Select evaluation window (last 7 available days)
    # ---------------------------------------------------
    eval_df = actual.tail(7).copy()

    # Same columns you use for forecasting
    feature_cols = [
        "TRIP_COUNT", "AVG_DISTANCE",
        "WEEKEND_TRIP_COUNT", "WEEKDAY_TRIP_COUNT",
        "AVG_DAILY_TEMPERATURE", "AVG_DAILY_HUMIDITY",
        "IS_PRECIP_DAY",
        "AVG_FARE_LAG1", "AVG_FARE_LAG7", "AVG_FARE_MA7"
    ]

    X_eval = eval_df[feature_cols].copy()
    y_actual = eval_df["AVG_FARE"].copy()

    # ---------------------------------------------------
    # Load trained model
    # ---------------------------------------------------
    model = joblib.load(MODEL_PATH)
    y_pred = model.predict(X_eval)

    # ---------------------------------------------------
    # Compute metrics
    # ---------------------------------------------------
    errors = y_actual - y_pred
    mape_vals = abs(errors / y_actual)

    mae = float(mean_absolute_error(y_actual, y_pred))
    mape_val = float(mape_vals.mean())

    # ---------------------------------------------------
    # Create evaluation output dataframe
    # ---------------------------------------------------
    df_out = pd.DataFrame({
        "EVAL_DATE": eval_df["TRIP_DATE"],
        "ACTUAL_AVG_FARE": y_actual,
        "PREDICTED_AVG_FARE": y_pred,
        "ERROR": errors,
        "MAPE": mape_vals
    })

    summary_row = pd.DataFrame([{
        "EVAL_DATE": None,
        "ACTUAL_AVG_FARE": None,
        "PREDICTED_AVG_FARE": None,
        "ERROR": mae,
        "MAPE": mape_val
    }])

    df_out = pd.concat([df_out, summary_row], ignore_index=True)

    # ---------------------------------------------------
    # Drop TMP table manually
    # ---------------------------------------------------
    with hook.get_conn() as conn, conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS USER_DB_CAT.ANALYTICS.FORECAST_EVAL_TMP")

    # ---------------------------------------------------
    # Write to Snowflake safely
    # ---------------------------------------------------
    df_out.to_sql(
        "FORECAST_EVAL_TMP",
        engine,
        index=False,
        if_exists="append",
        schema="ANALYTICS"
    )

    # ---------------------------------------------------
    # Final CTAS table
    # ---------------------------------------------------
    with engine.begin() as conn:
        conn.execute("""
            CREATE OR REPLACE TABLE USER_DB_CAT.ANALYTICS.FORECAST_EVAL AS
            SELECT * FROM USER_DB_CAT.ANALYTICS.FORECAST_EVAL_TMP
        """)

    print("Evaluation table built using TRUE hindcasting.")


# -----------------------------
# DAG DEFINITION
# -----------------------------
with DAG(
    dag_id="forecast_evaluation_dag",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["ml", "evaluation"],
) as dag:

    PythonOperator(
        task_id="forecast_eval_ctas",
        python_callable=build_forecast_eval_ctas,
    )
