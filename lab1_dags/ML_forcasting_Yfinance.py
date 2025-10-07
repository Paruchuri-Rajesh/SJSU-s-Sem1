from airflow import DAG
from airflow.decorators import task
from datetime import datetime
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

# ------------------------------
# Snowflake connection function
# ------------------------------
def return_snowflake_conn():
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    conn = hook.get_conn()
    return conn.cursor()


# ------------------------------
# Task 1: Train - create view & ML forecast model
# ------------------------------
@task
def train(cur):
    try:
        # Create view from raw table
        cur.execute("""
        CREATE OR REPLACE VIEW raw.stock_prices_ml_view AS
        SELECT 
            TO_TIMESTAMP_NTZ(Date) AS forecast_date,
            Close,
            Symbol
        FROM raw.stock_prices_ml;
        """)

        # Create ML Forecast model
        cur.execute("""
        CREATE OR REPLACE SNOWFLAKE.ML.FORECAST raw.stock_price_forecast_model (
            INPUT_DATA => SYSTEM$REFERENCE('VIEW', 'raw.stock_prices_ml_view'),
            SERIES_COLNAME => 'SYMBOL',
            TIMESTAMP_COLNAME => 'forecast_date',
            TARGET_COLNAME => 'CLOSE',
            CONFIG_OBJECT => { 'ON_ERROR': 'SKIP' }
        );
        """)

    except Exception as e:
        print("Error in train task:", e)
        raise e


# ------------------------------
# Task 2: Predict - generate forecast & create final table
# ------------------------------
@task
def predict(cur):
    try:
        # Generate forecast for next 7 days and save temporary table
        cur.execute("""
        CALL raw.stock_price_forecast_model!FORECAST(
            FORECASTING_PERIODS => 7,
            CONFIG_OBJECT => {'prediction_interval': 0.95}
        );
        """)

        cur.execute("""
        CREATE OR REPLACE TABLE raw.stock_forecast_temp AS
        SELECT *
        FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));
        """)

        # Combine historical data with forecast into final Analytics table
        cur.execute("""
        CREATE OR REPLACE TABLE analytics.stock_forecast_final AS
        SELECT 
            SYMBOL, 
            DATE, 
            CLOSE AS actual, 
            NULL AS forecast, 
            NULL AS lower_bound, 
            NULL AS upper_bound
        FROM raw.stock_prices_ml
        UNION ALL
        SELECT 
            REPLACE(TO_VARCHAR(SERIES), '"', '') AS SYMBOL,
            TS AS DATE,
            NULL AS actual,
            FORECAST AS forecast,
            LOWER_BOUND AS lower_bound,
            UPPER_BOUND AS upper_bound
        FROM raw.stock_forecast_temp;
        """)

    except Exception as e:
        print("Error in predict task:", e)
        raise e


# ------------------------------
# DAG Definition
# ------------------------------
with DAG(
    dag_id='stock_ml_forecast',
    start_date=datetime(2025, 10, 2),
    catchup=False,
    schedule='30 2 * * *',
    tags=['ML', 'ETL']
) as dag:

    cur = return_snowflake_conn()

    # Define tasks
    training_task = train(cur)
    predicting_task = predict(cur)

    # Set task dependency
    training_task >> predicting_task
