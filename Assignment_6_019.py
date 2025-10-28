from airflow.decorators import task
from airflow import DAG
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime
import logging

# -----------------------------
# Snowflake helper
# -----------------------------
def get_snowflake_cursor():
    """Return Snowflake cursor using Airflow connection"""
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    conn = hook.get_conn()
    return conn.cursor()

# -----------------------------
# Task: Create Table As Select (CTAS)
# -----------------------------
@task
def run_ctas(schema, table, select_sql, primary_key=None):
    cur = get_snowflake_cursor()
    try:
        # Step 1: Create temp table
        temp_sql = f"CREATE OR REPLACE TABLE {schema}.temp_{table} AS {select_sql}"
        print(f"Creating temp table: {schema}.temp_{table}")
        cur.execute(temp_sql)

        # Step 2: Check primary key uniqueness
        if primary_key:
            check_sql = f"""
                SELECT {primary_key}, COUNT(1) AS cnt
                FROM {schema}.temp_{table}
                GROUP BY {primary_key}
                ORDER BY cnt DESC
                LIMIT 1
            """
            cur.execute(check_sql)
            result = cur.fetchone()
            if int(result[1]) > 1:
                raise Exception(f"Primary key {primary_key} not unique! {result}")

        # Step 3: Simple duplicate row check
        
        dup_sql = f"""
            SELECT COUNT(*) - COUNT(DISTINCT sessionId, userId, channel, ts) AS duplicate_count
            FROM {schema}.temp_{table}
        """
        cur.execute(dup_sql)
        dup_result = cur.fetchone()
        if dup_result[0] > 0:
            raise Exception(f"{dup_result[0]} duplicate rows found in temp table!")


        # Step 4: Swap temp table with main table
        cur.execute(f"CREATE TABLE IF NOT EXISTS {schema}.{table} AS SELECT * FROM {schema}.temp_{table} WHERE 1=0;")
        cur.execute(f"ALTER TABLE {schema}.{table} SWAP WITH {schema}.temp_{table};")
        print(f"Table {schema}.{table} created successfully!")

    except Exception as e:
        print("Error:", e)
        raise

# -----------------------------
# DAG Definition
# -----------------------------
with DAG(
    dag_id='build_session_summaryy',
    start_date=datetime(2024, 10, 2),
    schedule='55 2 * * *',  # Runs daily at 02:45 AM
    catchup=False,
    tags=['ELT']
) as dag:

    schema = "analytics"
    table = "session_summary"
    select_sql = """
        SELECT u.*, s.ts
        FROM raw.user_session_channel u
        JOIN raw.session_timestamp s ON u.sessionId = s.sessionId
    """

    run_ctas(schema, table, select_sql, primary_key='sessionId')
