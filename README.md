# NYC Taxi Data Warehouse with dbt & Airflow

End-to-end data warehouse solution for NYC taxi trip analysis combining historical trip data with real-time weather observations.

## Architecture

```
Data Sources → Airflow ETL → Snowflake (RAW) → dbt Transformations → Snowflake (ANALYTICS) → BI Tools
```

### Components

1. **ETL Layer (Airflow)**
   - `nyc_taxi_pyspark_etl`: Downloads and processes historical NYC taxi trip data using PySpark
   - `nyc_weather_realtime_etl`: Fetches real-time weather data from OpenWeather API

2. **Transformation Layer (dbt)**
   - **Staging**: Clean and standardize raw data
   - **Intermediate**: Add business logic and derived metrics
   - **Marts**: Final analytics tables for BI consumption

3. **Storage (Snowflake)**
   - **RAW schema**: Raw data from ETL pipelines
   - **ANALYTICS schema**: Transformed, analytics-ready tables

---

## dbt Models

### Model Layers

#### Staging (`models/staging/`)
Light transformations and data quality filters on raw data:

- **stg_taxi_trips**: Cleaned NYC taxi trip data
  - Filters invalid records (null timestamps, negative amounts)
  - Standardizes column names
  - Source: `RAW.NYC_TAXI_TRIPS`

- **stg_weather**: Standardized weather observations
  - Renames columns for consistency
  - Filters missing critical data
  - Source: `RAW.RAW_WEATHER`

#### Intermediate (`models/intermediate/`)
Business logic and enrichments:

- **int_trips_enriched**: Trip data with calculated metrics
  - Trip duration in minutes
  - Average speed (mph)
  - Time-based features (hour_of_day, day_of_week, is_weekend)
  - Anomaly flags (duration, distance, passenger count)

- **int_weather_hourly**: Weather aggregated to hourly grain
  - Handles multiple observations per hour
  - Calculates min/max/avg temperatures
  - Takes most recent weather description

#### Marts (`models/marts/`)
Final analytics tables for dashboards:

- **mart_trips_weather**: Hourly trip metrics joined with weather
  - Trip count, avg distance, avg fare per hour
  - Weather conditions (temperature, humidity, description)
  - Time dimensions for analysis

- **mart_daily_metrics**: Daily aggregated trip and revenue metrics
  - Total revenue, trip count
  - Average fare, distance, duration
  - Weekend vs weekday breakdown
  - Daily weather summary

- **mart_zone_analysis**: Zone-level trip patterns
  - Popular pickup/dropoff zone pairs
  - Average metrics by zone pair
  - Peak hour identification

---

## Setup Instructions

### Prerequisites

- Docker & Docker Compose
- Snowflake account with database and warehouse
- OpenWeather API key (for weather DAG)

### 1. Clone and Navigate

```bash
cd nyc-taxi-data-warehouse
```

### 2. Configure Snowflake Connection

Set Snowflake environment variables (or use Airflow UI after startup):

```bash
export SNOWFLAKE_ACCOUNT="your_account"
export SNOWFLAKE_USER="your_user"
export SNOWFLAKE_PASSWORD="your_password"
export SNOWFLAKE_ROLE="your_role"
export SNOWFLAKE_DATABASE="your_database"
export SNOWFLAKE_WAREHOUSE="your_warehouse"
```

### 3. Build and Start Services

```bash
# Build Docker image with dbt included
docker-compose build

# Start Airflow and PostgreSQL
docker-compose up -d

# Check services are running
docker ps
```

### 4. Configure Airflow

Access Airflow UI at `http://localhost:8081` (username/password: `airflow/airflow`)

**Add Snowflake Connection:**
- Go to Admin → Connections
- Add new connection:
  - Conn ID: `snowflake_catfish`
  - Conn Type: `Snowflake`
  - Fill in account, user, password, warehouse, database, role

**Add Airflow Variables:**
- `target_schema_raw`: `RAW`
- `openweather_api_key`: `your_api_key`
- `weather_city`: `New York`

### 5. Run ETL Pipelines

1. Unpause `nyc_taxi_pyspark_etl` DAG
2. Unpause `nyc_weather_realtime_etl` DAG
3. Wait for initial data load to RAW schema

### 6. Run dbt Transformations

Unpause `dbt_transformation_pipeline` DAG - it will:
1. Create ANALYTICS schema
2. Install dbt dependencies
3. Check source data freshness
4. Run all dbt models (staging → intermediate → marts)
5. Execute data quality tests
6. Generate documentation

---

## dbt Development

### Local Development

```bash
# Navigate to dbt project
cd nyc_taxi_data_warehouse_elt

# Install dependencies
dbt deps

# Test connection
dbt debug --profiles-dir .

# Run models
dbt run --profiles-dir .

# Run tests
dbt test --profiles-dir .

# Run specific model
dbt run --select stg_taxi_trips --profiles-dir .

# Run marts only
dbt run --select marts.* --profiles-dir .
```

### Adding New Models

1. Create SQL file in appropriate directory (staging/intermediate/marts)
2. Use dbt ref() function to reference upstream models:
   ```sql
   select * from {{ ref('stg_taxi_trips') }}
   ```
3. Document model in schema.yml
4. Add tests in schema.yml
5. Test locally: `dbt run --select your_model`

### Model Materialization

- **Staging**: Views (fast, always fresh)
- **Intermediate**: Views (lightweight transformations)
- **Marts**: Tables (performance for BI tools)

---

## Data Quality Tests

dbt tests ensure data integrity:

- **not_null**: Critical fields have values
- **unique**: Primary keys are unique
- **accepted_range**: Numeric values within valid ranges (dbt_utils)
- **relationships**: Foreign keys are valid
- **custom tests**: Business logic validation

Run tests:
```bash
dbt test --profiles-dir .
```

---

## Monitoring & Operations

### DAG Schedules

- **ETL DAGs**: Hourly (data ingestion)
- **dbt DAG**: Daily at 2 AM UTC (transformations)

### Data Freshness

dbt checks source data freshness:
- **Warn**: Data older than 2 hours
- **Error**: Data older than 6 hours

### Troubleshooting

**dbt connection issues:**
```bash
# Test Snowflake connection
dbt debug --profiles-dir /opt/airflow/dbt

# Check environment variables
docker exec nyc-taxi-data-warehouse-airflow-1 env | grep SNOWFLAKE
```

**Missing source data:**
- Verify ETL DAGs ran successfully
- Check RAW schema has data:
  ```sql
  SELECT COUNT(*) FROM RAW.NYC_TAXI_TRIPS;
  SELECT COUNT(*) FROM RAW.RAW_WEATHER;
  ```

**Model failures:**
- Check Airflow logs for dbt_run task
- Run model manually to debug:
  ```bash
  dbt run --select failing_model --profiles-dir . --debug
  ```

---

# Machine Learning Pipeline
# 5️⃣ Model Training DAG — train_fare_model_dag

This DAG:

Loads features from FARE_DAILY_FEATURES

Splits into train/test

Trains a Random Forest Regression model

Evaluates model accuracy (hindcast MAE ≈ 2–3 dollars)

Saves trained model → /opt/airflow/models/fare_model.pkl

Why Random Forest?

Handles non-linear relationships (fare depends on weather, distance, congestion)

Robust to outliers in real city data

No assumptions about distribution or time-series stationarity

Fast inference and retraining

Works well with mixed engineered features (lags, moving averages)

Output:

✔ fare_model.pkl
✔ Training logs

# 6️⃣ Forecast Generation DAG — fare_forecasting_dag

Loads trained model

Constructs the next 7 days of features

Predicts future average fare

Inserts results into:

Output Table:

ANALYTICS.FARE_DAILY_FORECAST

Sample Output:

FORECAST_DATE	PREDICTED_AVG_FARE
2025-11-02	28.39
2025-11-03	28.39
…	…
# 7️⃣ Forecast Evaluation DAG — forecast_evaluation_dag

Because actual values for future dates don’t exist, we evaluate using hindcasting:

Select last 7 historical days

Re-predict them using the model

Compute:

MAE

MAPE

Daily error values

Store as analytics table:

Output Table:

ANALYTICS.FORECAST_EVAL

# 8️⃣ Zone Demand & Weather Forecast CTAS DAGs

Produce analytic-ready aggregates for Tableau:

ZONE_DEMAND — zone-level daily demand & avg fare

WEATHER_FORECAST_DAILY — temp/humidity/precip summary

## Project Structure

```
nyc-taxi-data-warehouse/
├── dags/
│   ├── etl_spark_historical.py
│   ├── weather_realtime_etl.py
│   ├── weather_historical_backfill.py
│   ├── dbt_transformation_dag.py
│   ├── train_fare_model_dag.py
│   ├── fare_forecasting_dag.py
│   ├── forecast_evaluation_dag.py
│   ├── zone_forecast.py
│   ├── weather_future_realtime.py
├── nyc_taxi_data_warehouse_elt/      # dbt project (NEW)
│   ├── models/
│   │   ├── staging/                   # Staging models + sources
│   │   ├── intermediate/              # Business logic layer
│   │   └── marts/                     # Analytics tables
│   ├── dbt_project.yml               # dbt configuration
│   ├── profiles.yml                  # Snowflake connection
│   └── packages.yml                  # dbt dependencies
├── Dockerfile                         # Includes dbt packages
├── docker-compose.yaml               # Airflow + dbt volumes
└── README.md                         # This file
```

---

## Next Steps

1. ✅ ETL pipelines loading to RAW schema
2. ✅ dbt models transforming to ANALYTICS schema
3. 🔄 **Connect BI tool** (Tableau, Power BI, Looker)
4. 🔄 **Add ML forecasting** using Snowflake ML functions
5. 🔄 **Incremental models** for large fact tables
6. 🔄 **Data quality monitoring** dashboard

---

## References

- [dbt Documentation](https://docs.getdbt.com/)
- [Snowflake dbt Adapter](https://docs.getdbt.com/docs/core/connect-data-platform/snowflake-setup)
- [Airflow dbt Integration](https://airflow.apache.org/docs/)
- [NYC TLC Trip Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)
