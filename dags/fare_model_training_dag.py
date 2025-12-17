from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime, timedelta
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error
import joblib
import os

SNOWFLAKE_CONN_ID = "SNOWFLAKE_CONN_ID"
MODEL_PATH = "/opt/airflow/models/fare_model.pkl"

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

def train_fare_model(**context):

    # 1) Get training data from Snowflake
    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    engine = hook.get_sqlalchemy_engine()

    query = """
        SELECT
            TRIP_DATE,
            AVG_FARE,
            TRIP_COUNT,
            AVG_DISTANCE,
            WEEKEND_TRIP_COUNT,
            WEEKDAY_TRIP_COUNT,
            AVG_DAILY_TEMPERATURE,
            AVG_DAILY_HUMIDITY,
            IS_PRECIP_DAY,
            AVG_FARE_LAG1,
            AVG_FARE_LAG7,
            AVG_FARE_MA7
        FROM USER_DB_CAT.ANALYTICS.FARE_DAILY_FEATURES
        ORDER BY TRIP_DATE;
    """

    df = pd.read_sql(query, engine)
    df.columns = df.columns.str.upper()  # standardize column names

    # Drop rows with missing lag values
    df = df.dropna().reset_index(drop=True)

    # 2) Define target + features
    target = "AVG_FARE"
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

    X = df[feature_cols]
    y = df[target]

    # Time-based split
    split_idx = int(len(df) * 0.7)
    X_train, X_test = X.iloc[:split_idx], X.iloc[split_idx:]
    y_train, y_test = y.iloc[:split_idx], y.iloc[split_idx:]

    # 3) Train model
    model = RandomForestRegressor(
        n_estimators=200,
        random_state=42,
        min_samples_leaf=2
    )
    model.fit(X_train, y_train)

    # 4) Evaluate model
    y_pred = model.predict(X_test)
    mae = mean_absolute_error(y_test, y_pred)

    print("TRAINING COMPLETE")
    print(f"Test MAE: {mae:.3f}")

    # 5) Save model
    os.makedirs("/opt/airflow/models", exist_ok=True)
    joblib.dump(model, MODEL_PATH)
    print(f"Saved model to {MODEL_PATH}")

    return f"Model trained and saved. MAE={mae:.3f}"

with DAG(
    dag_id="fare_model_training_dag",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",   # you can change to @weekly if desired
    catchup=False,
    default_args=default_args,
    tags=["ml", "forecast", "training"],
) as dag:

    train_model = PythonOperator(
        task_id="train_fare_model",
        python_callable=train_fare_model,
    )
