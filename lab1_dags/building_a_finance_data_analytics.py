# -----------------------------
# Import Libraries
# -----------------------------
from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime, timedelta
import pandas as pd
import yfinance as yf
import snowflake.connector
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

# -----------------------------
# Snowflake Helper Function
# -----------------------------
# This function establishes a connection to Snowflake using Airflow's SnowflakeHook
# It returns a cursor object that can be used to execute SQL queries
def return_snowflake_conn():
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    return hook.get_conn().cursor()

# -----------------------------
# Step 1: Extract Task
# -----------------------------
@task
def extract_stock_data(symbols):
    
    print(f"Extracting data for symbols: {symbols}")
    data = yf.download(symbols, period="180d")
    print(f"Extracted {data.shape[0]} rows")
    return data

# -----------------------------
# Step 2: Transform Task
# -----------------------------
@task
def transform_stock_data(data, symbols):
    
    data_reset = data.reset_index()
    # Flatten columns
    data_reset.columns = ['Date'] + [f"{price}_{symbol}" for price, symbol in data_reset.columns[1:]]
    flattened_rows = []
    for idx, row in data_reset.iterrows():
        for symbol in symbols:
            flattened_rows.append({
                "Date": row['Date'],
                "symbol": symbol,
                "open": row[f"Open_{symbol}"],
                "high": row[f"High_{symbol}"],
                "low": row[f"Low_{symbol}"],
                "close": row[f"Close_{symbol}"],
                "volume": row[f"Volume_{symbol}"]
            })
    df_flat = pd.DataFrame(flattened_rows)
    df_flat['Date'] = pd.to_datetime(df_flat['Date'])
    print(f"Transformed data shape: {df_flat.shape}")
    return df_flat

# -----------------------------
# Step 3: Load Task
# -----------------------------
@task
def load_stock_data(df, table_name="raw.stock_prices_ml"):
    
   
    df['Date'] = pd.to_datetime(df['Date'])
    cur = return_snowflake_conn()
    try:
        cur.execute("BEGIN;")
        cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            symbol VARCHAR,
            Date DATE,
            open FLOAT,
            high FLOAT,
            low FLOAT,
            close FLOAT,
            volume BIGINT,
            PRIMARY KEY(symbol, Date)
        );
        """)
        cur.execute(f"DELETE FROM {table_name};")  # full refresh
        print(f"Deleted old records from {table_name}.")
        
        for r in df.to_dict(orient='records'):
            symbol_safe = r['symbol'].replace("'", "''")
            cur.execute(f"""
                INSERT INTO {table_name} (symbol, Date, open, high, low, close, volume)
                VALUES (
                    '{symbol_safe}',
                    '{r['Date'].strftime('%Y-%m-%d')}',
                    {r['open']}, {r['high']}, {r['low']}, {r['close']}, {r['volume']}
                );
            """)
        cur.execute("COMMIT;")
        print(f"Successfully loaded {len(df)} rows into {table_name}.")
    except Exception as e:
        cur.execute("ROLLBACK;")
        print("Error during load, transaction rolled back:", e)
        raise e
    finally:
        cur.close()

# -----------------------------
# Step 4: DAG Definition
# -----------------------------
with DAG(
    dag_id='yfinance_etl_dag',
    start_date=datetime(2025, 10, 1),
    catchup=False,
    schedule_interval='0 2 * * *',  # daily 2:00 AM
    tags=['ETL', 'Stock'],
    default_args={
        'owner': 'airflow',
        'retries': 1,
        'retry_delay': timedelta(minutes=5)
    }
) as dag:
    
     # Fetch Airflow Variables
    import json
    symbols = json.loads(Variable.get("stock_symbols"))
    print(symbols)

    #symbols = ["TSLA", "NVDA"]
    #symbol = Variable.get("stock_symbol", default_var="AAPL")  # Can be parameterized

# Define task dependencies
    extracted_data = extract_stock_data(symbols)
    transformed_data = transform_stock_data(extracted_data, symbols)
    loaded_data =load_stock_data(transformed_data)

    # New Trigger Task — triggers your ML forecast DAG after ETL completes
    trigger_ml_dag = TriggerDagRunOperator(
        task_id='trigger_stock_ml_forecast',
        trigger_dag_id='stock_ml_forecast',  # must match the ML DAG’s ID
        wait_for_completion=False
    )

    #  Task dependency chain
    extracted_data >> transformed_data >> loaded_data >> trigger_ml_dag
