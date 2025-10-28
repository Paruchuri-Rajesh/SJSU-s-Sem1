from airflow.decorators import task
from airflow import DAG
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime
import logging

# -----------------------------
# Helper Function
# -----------------------------
def get_snowflake_cursor():
    """Returns a Snowflake cursor using Airflow connection"""
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    conn = hook.get_conn()
    return conn.cursor()

# -----------------------------
# Task 1: Import user_session_channel
# -----------------------------
@task
def import_user_session_channel():
    cur = get_snowflake_cursor()
    try:
        # Create table
        cur.execute("""
            CREATE TABLE IF NOT EXISTS raw.user_session_channel (
                userId INT NOT NULL,
                sessionId VARCHAR(32) PRIMARY KEY,
                channel VARCHAR(32) DEFAULT 'direct'
            )
        """)
        logging.info("user_session_channel table created successfully.")

        # Copy data from stage
        cur.execute("""
            COPY INTO raw.user_session_channel
            FROM @raw.blob_stage/user_session_channel.csv
        """)
        logging.info("user_session_channel data imported successfully.")

    except Exception as e:
        logging.error(f"Failed to import user_session_channel: {e}")
        raise
    finally:
        cur.close()

# -----------------------------
# Task 2: Import session_timestamp
# -----------------------------
@task
def import_session_timestamp():
    cur = get_snowflake_cursor()
    try:
        # Create table
        cur.execute("""
            CREATE TABLE IF NOT EXISTS raw.session_timestamp (
                sessionId VARCHAR(32) PRIMARY KEY,
                ts TIMESTAMP
            )
        """)
        logging.info("session_timestamp table created successfully.")

        # Copy data from stage
        cur.execute("""
            COPY INTO raw.session_timestamp
            FROM @raw.blob_stage/session_timestamp.csv
        """)
        logging.info("session_timestamp data imported successfully.")

    except Exception as e:
        logging.error(f"Failed to import session_timestamp: {e}")
        raise
    finally:
        cur.close()
# -----------------------------
# DAG Definition
# -----------------------------
with DAG(
    dag_id='import_raw_tab',
    start_date=datetime(2024, 10, 2),
    schedule='45 2 * * *',  # Daily at 2:45 AM
    catchup=False,
    tags=['ELT']
) as dag:

    # Task dependencies
    task_user = import_user_session_channel()
    task_timestamp = import_session_timestamp()
