from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from datetime import datetime

SNOWFLAKE_CONN_ID = "snowflake_catfish"

SQL_CREATE_OBJECTS = """
CREATE SCHEMA IF NOT EXISTS RAW;

CREATE TABLE IF NOT EXISTS RAW.USER_SESSION_CHANNEL (
    userId INT NOT NULL,
    sessionId VARCHAR(32) PRIMARY KEY,
    channel VARCHAR(32) DEFAULT 'direct'
);

CREATE TABLE IF NOT EXISTS RAW.SESSION_TIMESTAMP (
    sessionId VARCHAR(32) PRIMARY KEY,
    ts TIMESTAMP
);

-- Create/replace stage that points to public S3 (assignment example)
CREATE OR REPLACE STAGE RAW.BLOB_STAGE
  URL = 's3://s3-geospatial/readonly/'
  FILE_FORMAT = (TYPE = CSV, SKIP_HEADER = 1, FIELD_OPTIONALLY_ENCLOSED_BY = '"');
"""

SQL_COPY_USER_SESSION_CHANNEL = """
COPY INTO RAW.USER_SESSION_CHANNEL (userId, sessionId, channel)
FROM (
  SELECT $1::INT, $2::VARCHAR(32), $3::VARCHAR(32)
  FROM @RAW.BLOB_STAGE/user_session_channel.csv
)
ON_ERROR = 'ABORT_STATEMENT'
FORCE = TRUE;
"""

SQL_COPY_SESSION_TIMESTAMP = """
COPY INTO RAW.SESSION_TIMESTAMP (sessionId, ts)
FROM (
  SELECT $1::VARCHAR(32), $2::TIMESTAMP
  FROM @RAW.BLOB_STAGE/session_timestamp.csv
)
ON_ERROR = 'ABORT_STATEMENT'
FORCE = TRUE;
"""

with DAG(
    dag_id="BuildSummary",           # per instructions
    start_date=datetime(2024, 10, 2),
    schedule_interval=None,          # run on-demand; change if you need a schedule
    catchup=False,
    tags=["ELT", "snowflake"],
) as dag:

    ensure_raw_objects = SnowflakeOperator(
        task_id="ensure_raw_schema_tables_stage",
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql=SQL_CREATE_OBJECTS,
    )

    copy_user_session_channel = SnowflakeOperator(
        task_id="copy_user_session_channel",
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql=SQL_COPY_USER_SESSION_CHANNEL,
    )

    copy_session_timestamp = SnowflakeOperator(
        task_id="copy_session_timestamp",
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql=SQL_COPY_SESSION_TIMESTAMP,
    )

    ensure_raw_objects >> [copy_user_session_channel, copy_session_timestamp]
