import websocket
import json
import logging
import psycopg2
from psycopg2 import Error
from datetime import datetime
from dotenv import load_dotenv
import os

load_dotenv()

host = os.getenv('host')
db_name = os.getenv('database')
db_user = os.getenv('user')
db_password = os.getenv('password')

conn_params = {
    'host': host,
    'database': db_name,
    'user': db_user,
    'password': db_password
}

SOCKET = 'wss://stream.binance.com:9443/ws/pepeusdt@kline_15m'

CREATE_LOGS_TABLE_SQL = """
    CREATE TABLE IF NOT EXISTS {} (
        id SERIAL PRIMARY KEY,
        log_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        log_level VARCHAR(10),
        log_message TEXT
    );
"""

CREATE_TRADING_DATA_TABLE_SQL = """
    CREATE TABLE IF NOT EXISTS {} (
        id SERIAL PRIMARY KEY,
        event_type VARCHAR(50),
        event_time BIGINT,
        symbol VARCHAR(20),
        kline_start_time BIGINT,
        kline_close_time BIGINT,
        interval VARCHAR(10),
        first_trade_id BIGINT,
        last_trade_id BIGINT,
        open_price DECIMAL,
        close_price DECIMAL,
        high_price DECIMAL,
        low_price DECIMAL,
        volume DECIMAL,
        number_of_trades INT,
        is_kline_closed BOOLEAN,
        quote_asset_volume DECIMAL,
        taker_buy_base_asset_volume DECIMAL,
        taker_buy_quote_asset_volume DECIMAL,
        ignore INT
    );
"""

class PostgreSQLHandler(logging.Handler):
    def __init__(self, conn_params, table_name):
        logging.Handler.__init__(self)
        self.conn_params = conn_params
        self.table_name = table_name
        self.conn = None
        self.create_logs_table()

    def create_logs_table(self):
        try:
            self.conn = psycopg2.connect(**self.conn_params)
            cursor = self.conn.cursor()
            cursor.execute(CREATE_LOGS_TABLE_SQL.format(self.table_name))
            self.conn.commit()
            cursor.close()
            logging.info(f"Created {self.table_name} table if not exists.")
        except Error as e:
            logging.error(f"Error creating logs table: {e}")

    def emit(self, record):
        if not self.conn or self.conn.closed:
            self.create_logs_table()  
        log_entry = self.format(record)
        insert_sql = f"""
            INSERT INTO {self.table_name} (log_level, log_message)
            VALUES (%s, %s)
        """
        try:
            cursor = self.conn.cursor()
            cursor.execute(insert_sql, (record.levelname, log_entry))
            self.conn.commit()
            cursor.close()
        except Error as e:
            logging.error(f"Error inserting log record: {e}")


logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

def connect_postgresql():
    try:
        conn = psycopg2.connect(**conn_params)
        logger.info("Connected to PostgreSQL.")
        return conn
    except Error as e:
        logger.error(f"Error connecting to PostgreSQL: {e}")
        return None

def create_table(conn, table_name, sql):
    try:
        cursor = conn.cursor()
        cursor.execute(sql.format(table_name))
        conn.commit()
        logger.info(f"Created {table_name} table if not exists.")
    except Error as e:
        logger.error(f"Error creating table: {e}")

def insert_row(conn, table_name, row):
    sql = f"""
        INSERT INTO {table_name} (
            event_type, event_time, symbol, kline_start_time, kline_close_time,
            interval, first_trade_id, last_trade_id, open_price, close_price,
            high_price, low_price, volume, number_of_trades, is_kline_closed,
            quote_asset_volume, taker_buy_base_asset_volume, taker_buy_quote_asset_volume,
            ignore
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    try:
        cursor = conn.cursor()
        cursor.execute(sql, row)
        conn.commit()
        logger.info(f"Received message: {row}")
    except Error as e:
        logger.error(f"Error inserting row: {e}")

def on_open(ws):
    logger.info('WebSocket connected.')

def on_close(ws, close_status_code, close_msg):
    logger.warning(f'WebSocket connection closed: {close_status_code} - {close_msg}')

def on_error(ws, error):
    logger.error(f'WebSocket error: {error}')

running = True 
table_counter = 1  

def on_message(ws, message):
    global running, table_counter
    
    if not running:
        return 
    
    json_message = json.loads(message)
    logger.debug(json_message)  
    
    # Determine table names with counter suffix
    log_table_name = f"pepeusdt_15m_{table_counter}log"
    trading_table_name = f"pepeusdt_15m_{table_counter}"
    
    # Create tables if not exists
    create_table(conn, log_table_name, CREATE_LOGS_TABLE_SQL)
    create_table(conn, trading_table_name, CREATE_TRADING_DATA_TABLE_SQL)
    
    # Insert data into tables
    insert_row(conn, trading_table_name, get_trading_data_row(json_message))
    insert_row(conn, log_table_name, get_log_data_row(json_message))
    
def get_trading_data_row(json_message):
    kline = json_message['k']
    return (
        json_message['e'],          # event_type
        json_message['E'],          # event_time
        json_message['s'],          # symbol
        kline['t'],                 # kline_start_time
        kline['T'],                 # kline_close_time
        kline['i'],                 # interval
        kline['f'],                 # first_trade_id
        kline['L'],                 # last_trade_id
        kline['o'],                 # open_price
        kline['c'],                 # close_price
        kline['h'],                 # high_price
        kline['l'],                 # low_price
        kline['v'],                 # volume
        kline['n'],                 # number_of_trades
        kline['x'],                 # is_kline_closed
        kline['q'],                 # quote_asset_volume
        kline['V'],                 # taker_buy_base_asset_volume
        kline['Q'],                 # taker_buy_quote_asset_volume
        kline['B']                  # ignore
    )

def get_log_data_row(json_message):
    return (
        json_message['e'],          # event_type
        json_message['E'],          # event_time
        json_message['s'],          # symbol
        json.dumps(json_message)    # log_message (JSON format)
    )

def connect_websocket():
    websocket.enableTrace(False)  
    ws = websocket.WebSocketApp(SOCKET,
                                on_open=on_open,
                                on_close=on_close,
                                on_message=on_message,
                                on_error=on_error)
    return ws

if __name__ == "__main__":
    
    conn = connect_postgresql()
    if conn:
        
        log_table_name = f"pepeusdt_15m_{table_counter}_log"
        trading_table_name = f"pepeusdt_15m_{table_counter}"
        create_table(conn, log_table_name, CREATE_LOGS_TABLE_SQL)
        create_table(conn, trading_table_name, CREATE_TRADING_DATA_TABLE_SQL)
        
        ws = connect_websocket()
        ws.run_forever()
        
        running = False  
        ws.close()       
        conn.close()     
        pg_handler.close()  
        logger.info("Shutdown completed.")
        
        table_counter += 1
