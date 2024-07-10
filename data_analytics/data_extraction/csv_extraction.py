import psycopg2
from datetime import datetime
import pandas as pd
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


table_names = ['shibusdt_1m', 'shibusdt_5m', 'shibusdt_15m', 'pepeusdt_1m', 'pepeusdt_5m', 'pepeusdt_15m']

for i in table_names:
    
    query = f"SELECT * FROM {i}"

   
    with conn.cursor() as cursor:
        cursor.execute(query)
        result = cursor.fetchall()

        columns = [
            'event_type', 'event_time', 'symbol', 'kline_start_time', 'kline_close_time', 
            'interval', 'first_trade_id', 'last_trade_id', 'open_price', 'close_price', 
            'high_price', 'low_price', 'volume', 'number_of_trades', 'is_kline_closed',
            'quote_assest_volume', 'take_buy_base_asset_volume', 'taker_buy_quote_asset_volume', 'ignore']
        df = pd.DataFrame(result, columns=columns)

    # Save the DataFrame to a CSV file with a safe filename
    df.to_csv(f"data_analytics/datasets/{i}.csv", index=False)

# Close the database connection
conn.close()
