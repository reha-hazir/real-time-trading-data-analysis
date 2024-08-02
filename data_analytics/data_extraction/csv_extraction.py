import psycopg2
import pandas as pd
from dotenv import load_dotenv
import os
import regex as re
from sqlalchemy import create_engine

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


table_names = [
    'btcusdt_5malone'
    ]

for i in table_names:
    
    query = f"SELECT * FROM {i}"

   
    with conn.cursor() as cursor:
        cursor.execute(query)
        result = cursor.fetchall()

        columns = [
            'id', 'event_type', 'event_time', 'symbol', 'kline_start_time', 'kline_close_time', 
            'interval', 'first_trade_id', 'last_trade_id', 'open_price', 'close_price', 
            'high_price', 'low_price', 'volume', 'number_of_trades', 'is_kline_closed',
            'quote_assest_volume', 'take_buy_base_asset_volume', 'taker_buy_quote_asset_volume', 'ignore', 'latency']
        df = pd.DataFrame(result, columns=columns)

    df.to_csv(f"data_analytics/datasets/{i}.csv", index=False)


conn.close()
