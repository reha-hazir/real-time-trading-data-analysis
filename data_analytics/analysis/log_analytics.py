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


start_date_europe = pd.Timestamp('2024-07-13', tz='Europe/Berlin').tz_convert('UTC')
end_date_europe = pd.Timestamp('2024-07-17', tz='Europe/Berlin').tz_convert('UTC')

query = sql.SQL("""
    SELECT *
    FROM btcusdt_5m_13_07_24_22_18_log
    WHERE log_time >= %s
      AND log_Time <= %s
    ORDER BY id DESC
""")


query_str = query.as_string(conn)

df = pd.read_sql(query_str, conn, params=(start_date_europe, end_date_europe))

conn.close()


print(df.info())


