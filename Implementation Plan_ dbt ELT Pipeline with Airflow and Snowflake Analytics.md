# Problem Statement
The current NYC Taxi Data Warehouse project has raw data landing in Snowflake (RAW schema) from two ETL pipelines:
* Historical NYC taxi trip data (NYC_TAXI_TRIPS table)
* Real-time weather data (RAW_WEATHER table)
These raw tables need to be transformed using dbt into analytics-ready models, orchestrated by Airflow, and stored in a dedicated ANALYTICS schema in Snowflake. The solution requires integrating dbt with the existing Airflow/Snowflake infrastructure.
# Current State
## Infrastructure
* Airflow 2.10.1 running in Docker with LocalExecutor
* PostgreSQL as Airflow metadata database
* Snowflake connection configured via SnowflakeHook (connection ID: `snowflake_catfish`)
* Custom Dockerfile with PySpark, Snowflake provider, and other dependencies
* Two operational DAGs: `nyc_taxi_pyspark_etl` (hourly) and `nyc_weather_realtime_etl` (hourly)
## Data Sources (RAW Schema)
* **RAW.NYC_TAXI_TRIPS**: Contains pickup_datetime, dropoff_datetime, pickup_zone_id, dropoff_zone_id, passenger_count, trip_distance, total_amount, load_ts
* **RAW.NYC_TAXI_TRIPS_STG**: Staging table for COPY/MERGE operations
* **RAW.RAW_WEATHER**: Contains observed_at, city, temp_f, weather_desc, humidity_pct, raw_json, load_ts
* Both tables use TIMESTAMP_NTZ for temporal columns
## Missing Components
* No dbt project exists in the repository
* No dbt models, seeds, or tests
* No dbt-Airflow integration DAG
* No ANALYTICS schema in Snowflake
* No transformed/modeled data for BI consumption
# Proposed Solution
## Architecture Changes
Create a layered dbt transformation pipeline:
1. **Staging Layer** (stg_*): Light transformations, type casting, and column renaming from RAW tables
2. **Intermediate Layer** (int_*): Business logic transformations, joins, and aggregations
3. **Mart Layer** (mart_*): Final analytics-ready models in ANALYTICS schema for BI tools
## Implementation Steps
### 1. dbt Project Initialization
* Create dbt project structure in repository root: `dbt_project/`
* Configure `dbt_project.yml` with project settings:
    * Project name: `nyc_taxi_analytics`
    * Model paths, materialization strategies (views for staging, tables for marts)
    * Target schema: ANALYTICS
* Create `profiles.yml` template for Snowflake connection (using same credentials as Airflow)
* Define schema structure: staging (views), intermediate (ephemeral/views), marts (tables)
### 2. dbt Models Development
#### Staging Models (dbt_project/models/staging/)
* `stg_taxi_trips.sql`: Clean and standardize taxi trip data from RAW.NYC_TAXI_TRIPS
    * Select and rename columns with consistent naming (snake_case)
    * Add data quality filters (positive amounts, valid timestamps)
    * Add surrogate keys using dbt_utils.generate_surrogate_key()
* `stg_weather.sql`: Standardize weather observations from RAW.RAW_WEATHER
    * Extract and rename key fields
    * Parse JSON variant column if needed
    * Ensure timestamp consistency
#### Intermediate Models (dbt_project/models/intermediate/)
* `int_trips_enriched.sql`: Enrich trip data with derived metrics
    * Calculate trip duration (dropoff - pickup)
    * Compute average speed (distance / duration)
    * Add time-based features (hour_of_day, day_of_week, is_weekend)
    * Flag anomalies (very short/long trips, zero passengers)
* `int_weather_hourly.sql`: Aggregate weather to hourly grain
    * Handle multiple observations per hour (avg temp, latest description)
    * Fill gaps for missing hours if needed
#### Mart Models (dbt_project/models/marts/)
* `mart_trips_weather.sql`: Join trips with weather by time window
    * Match trip pickup_datetime to nearest weather observation (within 1 hour)
    * Aggregate trip metrics by hour and weather condition
    * Include: trip count, avg distance, avg fare, avg duration, temperature, weather description
* `mart_daily_metrics.sql`: Daily aggregations for dashboards
    * Daily trip volume, revenue, average metrics
    * Weather summary per day
* `mart_zone_analysis.sql` (optional): Zone-level analytics
    * Popular pickup/dropoff zones
    * Zone pair metrics (OD matrix aggregates)
#### Schema YML Files
* `models/staging/schema.yml`: Document staging models, columns, and basic tests
* `models/marts/schema.yml`: Document mart models with business descriptions
* Add data quality tests: not_null, unique, relationships, accepted_values
### 3. dbt Configuration Files
#### sources.yml (dbt_project/models/staging/sources.yml)
* Define RAW schema as source
* Declare NYC_TAXI_TRIPS and RAW_WEATHER as source tables
* Add freshness checks (warn_after: 2 hours, error_after: 6 hours)
#### macros/ (optional but recommended)
* Create utility macros for common transformations
* Example: macro for timezone conversions, date spine generation
### 4. Airflow Integration
#### Update Dockerfile
* Add dbt-core and dbt-snowflake to pip install list
* Install specific version compatible with Snowflake provider (dbt-core~=1.7, dbt-snowflake~=1.7)
#### Create dbt Profiles Configuration
* Store dbt profiles.yml in /opt/airflow/config/ or embed in DAG using environment variables
* Use Airflow Variables or connection secrets for Snowflake credentials
* Configure profiles to point to same Snowflake account/database as existing DAGs
#### Create New Airflow DAG (dags/dbt_transformation_dag.py)
* DAG schedule: Run after ETL DAGs complete (use ExternalTaskSensor or schedule offset)
* Suggested schedule: `0 2 * * *` (daily at 2 AM, after hourly ETLs have populated data)
* Tasks:
    1. `ensure_analytics_schema`: Create ANALYTICS schema if not exists
    2. `dbt_deps`: Install dbt packages (if using dbt_utils)
    3. `dbt_seed`: Load any reference data (optional, e.g., zone lookup tables)
    4. `dbt_run`: Execute all dbt models (or use dbt_run_operation for specific models)
    5. `dbt_test`: Run data quality tests
    6. `dbt_docs_generate` (optional): Generate documentation
* Use BashOperator or PythonOperator to invoke dbt commands
* Pass dbt profile/target using environment variables or --profiles-dir flag
* Add proper error handling and logging
#### DAG Dependencies
* Option 1: Use ExternalTaskSensor to wait for both `nyc_taxi_pyspark_etl` and `nyc_weather_realtime_etl` to complete
* Option 2: Use simple schedule offset (e.g., ETLs run hourly, dbt runs daily at specific time)
* Ensure dbt DAG only runs when sufficient raw data exists
### 5. Testing and Validation
#### dbt Tests
* Generic tests in schema.yml files:
    * not_null on primary keys and critical fields
    * unique on surrogate keys
    * relationships between fact and dimension tables
    * accepted_values for categorical fields (e.g., valid weather conditions)
* Singular tests (dbt_project/tests/) for custom business logic:
    * Check trip duration is positive
    * Verify weather-trip join coverage (e.g., >95% of trips have weather match)
#### Airflow Validation
* Test dbt DAG with `airflow dags test` command
* Run manual DAG trigger in Airflow UI
* Verify all tasks complete successfully
* Check Airflow logs for dbt output
#### Snowflake Validation
* Query ANALYTICS schema to verify models materialized correctly
* Check row counts match expectations
* Verify data quality (no nulls where unexpected, valid date ranges)
* Compare staging model counts with RAW table counts
* Validate join logic in mart_trips_weather (sample queries)
### 6. Documentation and Maintenance
#### README Updates
* Add dbt section to main README.md
* Document model lineage and business definitions
* Include instructions for:
    * Running dbt locally (for development)
    * Viewing dbt docs (if generated)
    * Adding new models
    * Troubleshooting common issues
#### Monitoring and Alerting
* Configure Airflow email alerts for dbt DAG failures
* Add dbt test failures to monitoring
* Set up data freshness alerts using dbt source freshness checks
#### Future Enhancements
* Incremental models for large fact tables (using dbt incremental materialization)
* Snapshots for slowly changing dimensions
* Additional mart models based on BI requirements
* Integration with BI tool (mentioned in original docs: Tableau, Power BI, etc.)
### 7. ML Forecasting with Snowflake ML Functions
Leverage Snowflake's native ML forecasting capabilities (SNOWFLAKE.ML.FORECAST) to predict future taxi demand and enable proactive decision-making.
#### ML Feature Preparation Models (dbt_project/models/ml_features/)
Create dedicated dbt models to prepare time-series data for ML forecasting:
* `ml_hourly_demand_features.sql`: Aggregate trip data to hourly granularity with features
    * Timestamp (hourly grain)
    * Trip count (target variable)
    * Average trip distance
    * Average fare amount
    * Weather features: temperature, humidity, weather condition categorical
    * Temporal features: hour_of_day, day_of_week, is_weekend, is_holiday (if holiday calendar available)
    * Rolling features: 24hr moving average of trips, 7-day moving average
    * Lag features: trips from previous hour, same hour previous day, same hour previous week
* `ml_daily_demand_features.sql`: Daily aggregation alternative for longer-term forecasting
    * Daily trip volume, revenue, average metrics
    * Daily weather summary (avg temp, dominant weather condition)
    * Calendar features: month, day_of_month, is_month_start, is_month_end
* `ml_zone_hourly_features.sql` (optional): Zone-level hourly demand for multi-series forecasting
    * Pickup zone ID as series identifier
    * Per-zone trip counts and metrics
    * Enables forecasting demand by neighborhood/zone
#### ML Model Training (SQL-based)
Create stored procedures or views in Snowflake to train and manage forecasting models:
* **Model 1: Citywide Hourly Demand Forecast**
    * Input: `ml_hourly_demand_features` (from ANALYTICS schema)
    * Target: Trip count per hour
    * Features: Weather (temp, humidity), temporal patterns, historical lags
    * Model name: `ANALYTICS.NYC_TAXI_HOURLY_FORECAST_MODEL`
    * SQL Example:
```SQL
CREATE OR REPLACE SNOWFLAKE.ML.FORECAST ANALYTICS.NYC_TAXI_HOURLY_FORECAST_MODEL (
  INPUT_DATA => SYSTEM$REFERENCE('TABLE', 'ANALYTICS.ML_HOURLY_DEMAND_FEATURES'),
  TIMESTAMP_COLNAME => 'HOUR_TIMESTAMP',
  TARGET_COLNAME => 'TRIP_COUNT',
  CONFIG_OBJECT => {
    'frequency': '1 hour',
    'prediction_interval': 0.95
  }
);
```
* **Model 2: Daily Demand Forecast** (for strategic planning)
    * Input: `ml_daily_demand_features`
    * Target: Daily trip volume
    * Forecast horizon: 14-30 days ahead
    * Model name: `ANALYTICS.NYC_TAXI_DAILY_FORECAST_MODEL`
* **Model 3: Multi-Series Zone-Level Forecast** (advanced)
    * Input: `ml_zone_hourly_features`
    * Series column: `pickup_zone_id`
    * Target: Trips per zone per hour
    * Enables zone-specific demand predictions
    * Model name: `ANALYTICS.NYC_TAXI_ZONE_FORECAST_MODEL`
#### ML Inference Models (dbt_project/models/ml_inference/)
Create dbt models that invoke trained ML models for predictions:
* `ml_hourly_forecast_next_24h.sql`: Generate 24-hour ahead forecasts
    * Call `NYC_TAXI_HOURLY_FORECAST_MODEL!FORECAST(24)` method
    * Returns: timestamp, forecast, lower_bound, upper_bound, prediction_interval
    * Materialize as table for dashboard consumption
* `ml_daily_forecast_next_30d.sql`: Generate 30-day ahead forecasts
    * Call `NYC_TAXI_DAILY_FORECAST_MODEL!FORECAST(30)` method
    * Used for capacity planning and resource allocation
* `ml_forecast_evaluation.sql`: Model performance tracking
    * Call `<model_name>!SHOW_EVALUATION_METRICS()` method
    * Track MAE, RMSE, MAPE, R-squared over time
    * Materialize metrics for monitoring dashboards
* `ml_feature_importance.sql`: Understand model drivers
    * Call `<model_name>!EXPLAIN_FEATURE_IMPORTANCE()` method
    * Identify which features (weather, time-of-day, etc.) most influence predictions
#### Airflow ML Orchestration (Update dbt_transformation_dag.py)
Extend the dbt transformation DAG to include ML workflow:
* Add task group: `ml_forecasting_workflow`
    * Task 1: `dbt_run_ml_features` - Build ML feature tables
    * Task 2: `train_ml_models` - Execute CREATE/REPLACE FORECAST model SQL (via SnowflakeOperator)
    * Task 3: `dbt_run_ml_inference` - Generate predictions using dbt inference models
    * Task 4: `validate_forecasts` - Check forecast quality (no nulls, reasonable ranges)
* Scheduling considerations:
    * ML feature prep: Run daily after mart models complete
    * Model retraining: Weekly or monthly (models can be expensive to train)
    * Inference: Daily or on-demand to generate fresh predictions
    * Use separate task groups with different schedules or sensors
* Alternative: Create separate DAG `ml_forecasting_dag.py` that depends on dbt_transformation_dag
    * Schedule: Daily at 3 AM (after dbt marts complete at 2 AM)
    * Enables independent ML workflow management
#### ML Model Registry and Versioning
* Use Snowflake object naming conventions for model versioning:
    * `NYC_TAXI_HOURLY_FORECAST_MODEL_V1`, `_V2`, etc.
    * Or use CREATE OR REPLACE with careful testing
* Store model metadata in dedicated table: `ANALYTICS.ML_MODEL_METADATA`
    * Columns: model_name, trained_at, training_data_start, training_data_end, eval_metrics (VARIANT), model_version
    * Enables tracking model lineage and performance over time
#### Use Cases and Business Value
* **Operational Planning**: Predict peak demand hours to optimize driver allocation
* **Weather-Based Adjustments**: Anticipate increased demand during rain/snow
* **Revenue Forecasting**: Project daily/weekly revenue for financial planning
* **Zone-Level Insights**: Identify high-demand neighborhoods for targeted operations
* **Anomaly Detection**: Compare actual vs predicted to detect unusual patterns (events, incidents)
#### ML Monitoring and Retraining Strategy
* **Model Performance Monitoring**:
    * Track forecast accuracy weekly: compare predictions vs actuals
    * Alert if MAE or MAPE exceeds threshold (e.g., >15% error)
    * Dashboard showing forecast vs actual time-series plots
* **Retraining Triggers**:
    * Scheduled retraining: Monthly to incorporate latest data
    * Performance-based: Retrain if error metrics degrade
    * Event-based: Retrain after major disruptions (e.g., pandemic, transit changes)
* **A/B Testing**:
    * Train new model version alongside production model
    * Compare performance on holdout data
    * Promote better-performing model to production
#### Testing and Validation
* **Feature Quality Tests** (in dbt schema.yml):
    * No nulls in timestamp or target columns
    * No duplicate timestamps
    * Reasonable value ranges (e.g., trip_count >= 0)
    * No gaps in time series (use dbt_utils.test_gaps)
* **Forecast Validation**:
    * Backtesting: Train on historical data, predict known future, measure accuracy
    * Forecast sanity checks: Values within reasonable bounds
    * Seasonality validation: Forecasts reflect expected daily/weekly patterns
* **Integration Testing**:
    * Test full pipeline: features → training → inference
    * Verify model objects created successfully in Snowflake
    * Confirm inference models produce expected output schema
# File Structure
```warp-runnable-command
nyc-taxi-data-warehouse/
├── dbt_project/
│   ├── dbt_project.yml
│   ├── profiles.yml (template)
│   ├── packages.yml (optional, for dbt_utils)
│   ├── models/
│   │   ├── staging/
│   │   │   ├── sources.yml
│   │   │   ├── schema.yml
│   │   │   ├── stg_taxi_trips.sql
│   │   │   └── stg_weather.sql
│   │   ├── intermediate/
│   │   │   ├── schema.yml
│   │   │   ├── int_trips_enriched.sql
│   │   │   └── int_weather_hourly.sql
│   │   ├── marts/
│   │   │   ├── schema.yml
│   │   │   ├── mart_trips_weather.sql
│   │   │   ├── mart_daily_metrics.sql
│   │   │   └── mart_zone_analysis.sql
│   │   ├── ml_features/ (NEW)
│   │   │   ├── schema.yml
│   │   │   ├── ml_hourly_demand_features.sql
│   │   │   ├── ml_daily_demand_features.sql
│   │   │   └── ml_zone_hourly_features.sql (optional)
│   │   └── ml_inference/ (NEW)
│   │       ├── schema.yml
│   │       ├── ml_hourly_forecast_next_24h.sql
│   │       ├── ml_daily_forecast_next_30d.sql
│   │       ├── ml_forecast_evaluation.sql
│   │       └── ml_feature_importance.sql
│   ├── macros/ (optional)
│   ├── tests/ (optional singular tests)
│   └── seeds/ (optional reference data)
├── sql/ (NEW - ML model training scripts)
│   ├── train_hourly_forecast_model.sql
│   ├── train_daily_forecast_model.sql
│   └── train_zone_forecast_model.sql
├── dags/
│   ├── etl_spark_historical.py
│   ├── weather_realtime_etl.py
│   ├── dbt_transformation_dag.py (NEW)
│   └── ml_forecasting_dag.py (NEW - optional separate ML DAG)
├── config/
│   └── dbt_profiles.yml (optional alternative location)
├── Dockerfile (UPDATE)
├── docker-compose.yaml (may need volume mount for dbt_project)
└── README.md (UPDATE)
```
# Key Decisions
1. **dbt Project Location**: Place in `dbt_project/` subdirectory to keep it organized and separate from Airflow code
2. **Materialization Strategy**: Views for staging (fast, always fresh), tables for marts (performance for BI)
3. **Schema Naming**: Use ANALYTICS schema for all mart models, keep staging/intermediate as views in same schema or separate STAGING schema
4. **Orchestration**: Single daily dbt DAG that runs all models sequentially, rather than separate DAGs per model
5. **Credentials**: Reuse existing Snowflake connection from Airflow, configure via environment variables or Airflow Variables
6. **Testing**: Implement dbt tests as separate Airflow task to fail DAG if data quality issues detected
# Success Criteria
## Core dbt Pipeline
- [ ] dbt project successfully initializes and connects to Snowflake
- [ ] All staging models run and create views in ANALYTICS schema
- [ ] All mart models run and create tables in ANALYTICS schema
- [ ] dbt tests pass with 0 failures
- [ ] Airflow dbt DAG runs successfully on schedule
- [ ] Transformed data is queryable in Snowflake and suitable for BI tool consumption
- [ ] Data lineage is clear and documented
- [ ] No performance degradation in existing ETL DAGs
## ML Forecasting Extension
- [ ] ML feature tables successfully built with proper time-series structure (no gaps, no duplicates)
- [ ] Snowflake ML FORECAST models train successfully without errors
- [ ] Hourly forecast model produces 24-hour predictions with reasonable accuracy (MAPE < 20%)
- [ ] Daily forecast model produces 30-day predictions for strategic planning
- [ ] Inference models materialize forecast tables that can be consumed by dashboards
- [ ] Model evaluation metrics are tracked and accessible for monitoring
- [ ] Feature importance analysis provides actionable insights on demand drivers
- [ ] ML workflow integrates seamlessly with dbt transformation pipeline
- [ ] Forecast data quality validated (no nulls, values within expected ranges)
- [ ] ML orchestration runs on appropriate schedule (daily inference, weekly/monthly retraining)
