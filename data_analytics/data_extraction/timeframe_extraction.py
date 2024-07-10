import pandas as pd
import psycopg2
from psycopg2 import sql
from dotenv import load_dotenv
import os

load_dotenv()

host = os.getenv('host')
db_name = os.getenv('database')
db_user = os.getenv('user')
db_password = os.getenv('password')


conn_details = {
    'host': host,
    'database': db_name,
    'user': db_user,
    'password': db_password
}

conn = psycopg2.connect(**conn_details)


start_date_europe = pd.Timestamp('2024-06-28', tz='Europe/Berlin').tz_convert('UTC')
end_date_europe = pd.Timestamp('2024-06-30 21:57:45', tz='Europe/Berlin').tz_convert('UTC')

query = sql.SQL("""
    SELECT
        *,
        TO_TIMESTAMP(event_time / 1000.0) AS event_time_utc  -- Assuming event_time is in milliseconds
    FROM pepeusdt_15m
    WHERE TO_TIMESTAMP(event_time / 1000.0) >= %s
      AND TO_TIMESTAMP(event_time / 1000.0) <= %s
    ORDER BY TO_TIMESTAMP(event_time / 1000.0) ASC
""")


query_str = query.as_string(conn)

df = pd.read_sql(query_str, conn, params=(start_date_europe, end_date_europe))

conn.close()

if df['event_time_utc'].dtype == 'datetime64[ns, UTC]':
    df['event_time_europe'] = df['event_time_utc'].dt.tz_convert('Europe/Berlin')
else:
    df['event_time_europe'] = pd.to_datetime(df['event_time_utc'], unit='ms').dt.tz_localize('UTC').dt.tz_convert('Europe/Berlin')

print(df.head())
