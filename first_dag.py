
from datetime import datetime, timedelta
import requests
import pandas as pd

from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

DAG_ID = "hm5_Rajesh"
SNOWFLAKE_CONN_ID = "snowflake_conn"
TARGET_TABLE = "RAW.STOCK_PRICES_RAW"   # change if needed


with DAG(
    dag_id=DAG_ID,
    start_date=datetime(2025, 10, 1),
    schedule_interval="0 2 * * *",   # daily at 02:00; set to None for manual
    catchup=False,
    default_args={"owner": "airflow", "retries": 1, "retry_delay": timedelta(minutes=5)},
    tags=["ETL", "Stock", "AlphaVantage", "Snowflake"],
    description="Alpha Vantage daily prices -> Snowflake full refresh",
) as dag:

    # Pull variables
    API_KEY = Variable.get("Alpha_Vantage_key")                      # make sure this exists
    SYMBOL = Variable.get("stock_symbol", default_var="AAPL").upper()

    @task
    def extract_stock_data(symbol: str, api_key: str):
        url = "https://www.alphavantage.co/query"
        params = {"function": "TIME_SERIES_DAILY", "symbol": symbol, "apikey": api_key}
        r = requests.get(url, params=params, timeout=60)
        r.raise_for_status()
        data = r.json()

        # Helpful guards
        if "Note" in data:
            raise RuntimeError(f"Alpha Vantage rate limit: {data['Note']}")
        if "Error Message" in data:
            raise RuntimeError(f"Alpha Vantage error: {data['Error Message']}")
        ts = data.get("Time Series (Daily)")
        if not ts:
            raise RuntimeError(f"No 'Time Series (Daily)' in response. Keys: {list(data.keys())[:5]}")

        return ts  # dict of {date_str: {...}}


    @task
    def transform_stock_data(time_series: dict, symbol: str):
        # Build records, normalize types, keep last ~90 trading days
        records = []
        for date_str, vals in time_series.items():
            records.append(
                {
                    "symbol": symbol,
                    "date": pd.to_datetime(date_str).strftime("%Y-%m-%d"),  # ISO string
                    "open": float(vals["1. open"]),
                    "high": float(vals["2. high"]),
                    "low": float(vals["3. low"]),
                    "close": float(vals["4. close"]),
                    "volume": int(vals["5. volume"]),
                }
            )

        # sort ascending and keep last 90
        records.sort(key=lambda r: r["date"])
        if len(records) > 90:
            records = records[-90:]

        return records  # JSON-serializable list of dicts


    @task
    def load_stock_data(rows, table_name: str = TARGET_TABLE):
        """
        rows: list like
          [{"symbol":"AAPL","date":"YYYY-MM-DD","open":..., "high":..., "low":..., "close":..., "volume":...}]
        """
        # normalize one more time defensively
        norm_rows = [
            {
                "SYMBOL": str(r["symbol"]),
                "DATE": str(r["date"])[:10],
                "OPEN": float(r["open"]),
                "HIGH": float(r["high"]),
                "LOW": float(r["low"]),
                "CLOSE": float(r["close"]),
                "VOLUME": int(r["volume"]),
            }
            for r in rows
        ]

        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
        conn = hook.get_conn()
        try:
            # make sure we control the tx
            try:
                conn.autocommit = False
            except Exception:
                pass

            cur = conn.cursor()
            try:
                cur.execute("BEGIN")

                # create target if needed
                cur.execute(
                    f"""
                    CREATE TABLE IF NOT EXISTS {table_name} (
                        SYMBOL STRING,
                        DATE   DATE,
                        OPEN   FLOAT,
                        HIGH   FLOAT,
                        LOW    FLOAT,
                        CLOSE  FLOAT,
                        VOLUME NUMBER
                    )
                    """
                )

                # staging then swap (fast, safe)
                cur.execute(f"CREATE OR REPLACE TEMPORARY TABLE {table_name}_STG LIKE {table_name}")

                insert_sql = f"""
                    INSERT INTO {table_name}_STG (SYMBOL, DATE, OPEN, HIGH, LOW, CLOSE, VOLUME)
                    VALUES (%(SYMBOL)s, TO_DATE(%(DATE)s), %(OPEN)s, %(HIGH)s, %(LOW)s, %(CLOSE)s, %(VOLUME)s)
                """
                cur.executemany(insert_sql, norm_rows)

                cur.execute(f"TRUNCATE TABLE {table_name}")
                cur.execute(f"INSERT INTO {table_name} SELECT * FROM {table_name}_STG")

                cur.execute("COMMIT")
                return f"Loaded {len(norm_rows)} rows into {table_name}"
            except Exception:
                cur.execute("ROLLBACK")
                raise
            finally:
                cur.close()
        finally:
            conn.close()

    extracted = extract_stock_data(SYMBOL, API_KEY)
    transformed = transform_stock_data(extracted, SYMBOL)
    load_stock_data(transformed)
