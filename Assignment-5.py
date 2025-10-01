# dags/assignment_alpha_simple_ctx.py
from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

from datetime import datetime
import requests

# Read the Snowflake connection id from a Variable
SNOWFLAKE_CONN_ID = Variable.get("SNOWFLAKE_CONN_ID", default_var="snowflake_catfish")

with DAG(
    dag_id="assignment_alpha_simple_ctx",
    start_date=datetime(2025, 9, 1),
    schedule="@daily",
    catchup=False,
    tags=["assignment", "alpha", "snowflake"],
) as dag:

    @task
    def extract() -> list[dict]:
        """Pull compact daily OHLCV from Alpha Vantage for a small symbol list."""
        api_key = Variable.get("ALPHAVANTAGE_API_KEY")  # must exist
        symbols_csv = Variable.get("ALPHAVANTAGE_SYMBOLS", default_var="AAPL,MSFT")
        symbols = [s.strip().upper() for s in symbols_csv.split(",") if s.strip()]

        base = "https://www.alphavantage.co/query"
        rows: list[dict] = []

        for sym in symbols:
            r = requests.get(
                base,
                params={
                    "function": "TIME_SERIES_DAILY",
                    "symbol": sym,
                    "outputsize": "compact",
                    "datatype": "json",
                    "apikey": api_key,
                },
                timeout=30,
            )
            r.raise_for_status()
            data = r.json()
            ts = data.get("Time Series (Daily)")
            if not ts:
                continue
            for ds, f in ts.items():
                try:
                    rows.append(
                        {
                            "SYMBOL": sym,
                            "PRICE_DATE": ds,
                            "OPEN": float(f.get("1. open", 0) or 0),
                            "HIGH": float(f.get("2. high", 0) or 0),
                            "LOW":  float(f.get("3. low",  0) or 0),
                            "CLOSE": float(f.get("4. close", 0) or 0),
                            "VOLUME": int(float(f.get("5. volume", 0) or 0)),
                            "NOTE": None,
                        }
                    )
                except Exception:
                    # skip malformed row
                    pass

        # simple sanity filter
        return [r for r in rows if r["PRICE_DATE"] and r["CLOSE"] and r["CLOSE"] > 0]
#Implement the same full refresh using SQL transaction 
    @task
    def load_full_refresh(rows: list[dict]) -> str:
        """
        Full refresh into RAW.STOCK_PRICES_AV:
          - CREATE SCHEMA/TABLE IF NOT EXISTS
          - TRUNCATE
          - INSERT (executemany)
          - COMMIT / ROLLBACK
        """
        schema = Variable.get("SNOWFLAKE_SCHEMA", default_var="RAW")
        table  = Variable.get("SNOWFLAKE_TABLE",  default_var="STOCK_PRICES_AV")

        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
        conn = hook.get_conn()
        cur  = conn.cursor()

        try:
            cur.execute("BEGIN")
            cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {schema}.{table} (
                    SYMBOL     STRING,
                    PRICE_DATE DATE,
                    OPEN       FLOAT,
                    HIGH       FLOAT,
                    LOW        FLOAT,
                    CLOSE      FLOAT,
                    VOLUME     NUMBER,
                    NOTE       STRING
                )
            """)
            cur.execute(f"TRUNCATE TABLE {schema}.{table}")

            if rows:
                insert_sql = f"""
                    INSERT INTO {schema}.{table}
                    (SYMBOL, PRICE_DATE, OPEN, HIGH, LOW, CLOSE, VOLUME, NOTE)
                    VALUES (%(SYMBOL)s, %(PRICE_DATE)s, %(OPEN)s, %(HIGH)s, %(LOW)s, %(CLOSE)s, %(VOLUME)s, %(NOTE)s)
                """
                cur.executemany(insert_sql, rows)

            cur.execute("COMMIT")
            return f"Loaded {len(rows)} rows into {schema}.{table}"
        except Exception:
            cur.execute("ROLLBACK")
            raise
        finally:
            cur.close()
            conn.close()

    # wiring
    load_full_refresh(extract())
