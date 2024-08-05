import psycopg2
from psycopg2 import Error
import logging
from dotenv import load_dotenv
import os

load_dotenv()

class Config:
    def __init__(self):
        self.host = os.getenv('host')
        self.db_name = os.getenv('database')
        self.db_user = os.getenv('user')
        self.db_password = os.getenv('password')

config = Config()

conn_params = {
    'host': config.host,
    'database': config.db_name,
    'user': config.db_user,
    'password': config.db_password
}

# Correct the logging configuration
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')

logger = logging.getLogger(__name__)

class DatabaseHandler:
    def __init__(self, table_name):
        self.table_name = table_name
        self.conn = self.connect_postgresql()
        if self.conn:
            self.create_table()

    def connect_postgresql(self):
        try:
            conn = psycopg2.connect(**conn_params)
            logging.info("Connected to PostgreSQL.")
            return conn
        except Error as e:
            logging.error(f"Error connecting to PostgreSQL: {e}")
            return None

    def create_table(self):
        create_table_sql = f"""
            CREATE TABLE IF NOT EXISTS {self.table_name} (
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
        try:
            cursor = self.conn.cursor()
            cursor.execute(create_table_sql)
            self.conn.commit()
            logging.info("Created trading_data table if not exists.")
        except Error as e:
            logging.error(f"Error creating table: {e}")

    def insert_row(self, data):
        insert_sql = f"""
            INSERT INTO {self.table_name} (
                date, time, open_price, current_price, high_price, low_price, volume, number_of_trades,
                buy_volume, sell_volume, usdt_buy, usdt_sell, total_volume, trade_count, average_price, volume_dominance
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        try:
            cursor = self.conn.cursor()
            cursor.execute(insert_sql, (
                data["date"], data["time"], data["open_price"], data["current_price"], data["high_price"], data["low_price"], 
                data["volume"], data["number_of_trades"], data["buy_volume"], data["sell_volume"], data["usdt_buy"], 
                data["usdt_sell"], data["total_volume"], data["trade_count"], data["average_price"], data["volume_dominance"]
            ))
            self.conn.commit()
            logging.info(f"Inserted row: {data}")
        except Error as e:
            logging.error(f"Error inserting row: {e}")
