import asyncio
import websockets
import json
import logging
from datetime import datetime
import psycopg2
from psycopg2 import Error
from dotenv import load_dotenv
import os
import time

load_dotenv()

class Config:
    def __init__(self):
        self.host = os.getenv('host')
        self.db_name = os.getenv('database')
        self.db_user = os.getenv('user')
        self.db_password = os.getenv('password')

config = Config()

symbol = 'btcusdt'
interval = '1m'
table_name = symbol + '_' + interval + '_ALONE'
current_date = datetime.today().strftime('%d-%m-%y')
trade_data = {"buy": 0, "sell": 0, "count": 0, "price_sum": 0, "quantity_sum": 0}
current_minute = None
last_price = None
kline_closed_event = asyncio.Event()
data_lock = asyncio.Lock()

conn_params = {
    'host': config.host,
    'database': config.db_name,
    'user': config.db_user,
    'password': config.db_password
}

CREATE_TRADING_DATA_TABLE_SQL = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        id SERIAL PRIMARY KEY,
        date DATE,
        time BIGINT,
        open_price DECIMAL,
        current_price DECIMAL,
        high_price DECIMAL,
        low_price DECIMAL,
        volume DECIMAL,
        number_of_trades INT,
        buy_volume DECIMAL,
        sell_volume DECIMAL,
        usdt_buy DECIMAL,
        usdt_sell DECIMAL,
        total_volume DECIMAL,
        trade_count INT,
        average_price DECIMAL,
        volume_dominance INT
    );
"""

class UnixTimestampFormatter(logging.Formatter):
    def formatTime(self, record, datefmt=None):
        ct = self.converter(record.created)
        if datefmt:
            s = time.strftime(datefmt, ct) 
        else:
            s = str(int(record.created * 1000))
        return s

KLINE_SOCKET = f'wss://stream.binance.com:9443/ws/{symbol}@kline_{interval}'
TRADE_SOCKET = f'wss://stream.binance.com:9443/ws/{symbol}@trade'

# Set up logging configuration
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    datefmt='%s',
                    force=True)
logger = logging.getLogger(__name__)

def connect_postgresql():
    try:
        conn = psycopg2.connect(**conn_params)
        logger.info("Connected to PostgreSQL.")
        return conn
    except Error as e:
        logger.error(f"Error connecting to PostgreSQL: {e}")
        return None

def create_table(conn):
    try:
        cursor = conn.cursor()
        cursor.execute(CREATE_TRADING_DATA_TABLE_SQL)
        conn.commit()
        logger.info("Created trading_data table if not exists.")
    except Error as e:
        logger.error(f"Error creating table: {e}")

def insert_row(conn, row):
    sql = f"""
        INSERT INTO {table_name} (
            date, time, open_price, current_price, high_price, low_price, volume, number_of_trades,
            buy_volume, sell_volume, usdt_buy, usdt_sell, total_volume, trade_count, average_price, volume_dominance
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    try:
        cursor = conn.cursor()
        cursor.execute(sql, row)
        conn.commit()
        logger.info(f"Inserted row: {row}")
    except Error as e:
        logger.error(f"Error inserting row: {e}")

async def kline_websocket(conn):
    global current_minute, last_price

    async with websockets.connect(KLINE_SOCKET) as websocket:
        async for message in websocket:
            json_message = json.loads(message)
            kline = json_message['k']
            event_time = json_message['E']
            kline_closed = kline['x']
            open_price = float(kline['o'])
            current_price = float(kline['c'])
            high_price = float(kline['h'])
            low_price = float(kline['l'])
            volume = float(kline['v'])
            number_of_trades = kline['n']

            if kline_closed:
                current_minute = event_time // 1000 // 60

                async with data_lock:
                    buy_volume = trade_data['buy']
                    sell_volume = trade_data['sell']
                    trade_count = trade_data['count']
                    average_price = (trade_data['price_sum'] / trade_data['quantity_sum']) if trade_data['quantity_sum'] > 0 else 0

                buy_usdt = buy_volume * average_price
                sell_usdt = sell_volume * average_price
                total_volume_btc = buy_volume + sell_volume

                # Determine volume dominance
                if sell_volume > buy_volume:
                    volume_dominance = -1
                elif buy_volume > sell_volume:
                    volume_dominance = 1
                else:
                    volume_dominance = 0

                row = (
                    current_date, event_time, open_price, current_price, high_price, low_price, volume, number_of_trades,
                    buy_volume, sell_volume, buy_usdt, sell_usdt, total_volume_btc, trade_count, average_price, volume_dominance
                )

                insert_row(conn, row)

                async with data_lock:
                    trade_data.clear()
                    trade_data.update({"buy": 0, "sell": 0, "count": 0, "price_sum": 0, "quantity_sum": 0})
                
                kline_closed_event.set()

async def trade_websocket():
    global last_price

    async with websockets.connect(TRADE_SOCKET) as websocket:
        try:
            async for message in websocket:
                trade = json.loads(message)

                trade_time = trade["T"]
                price_usdt = float(trade["p"])
                quantity = float(trade["q"])
                is_sell = trade["m"]
                
                async with data_lock:
                    if is_sell:
                        trade_data["sell"] += quantity
                    else:
                        trade_data["buy"] += quantity

                    trade_data["count"] += 1
                    trade_data["price_sum"] += price_usdt * quantity
                    trade_data["quantity_sum"] += quantity
                    last_price = price_usdt

        except (websockets.ConnectionClosedError, Exception) as e:
            logger.error(f"Connection closed or error: {e}")

async def main():
    conn = connect_postgresql()
    if conn:
        create_table(conn)
        await asyncio.gather(kline_websocket(conn), trade_websocket())
        conn.close()
        logger.info("Shutdown completed.")

if __name__ == "__main__":
    asyncio.run(main())
