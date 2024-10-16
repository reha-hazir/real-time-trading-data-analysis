import websocket
import json
import logging
import csv
import os
from datetime import datetime

symbol = 'pepeusdt'
interval = '1m'
current_date = datetime.today().strftime('%d-%m-%y')
SOCKET = f'wss://stream.binance.com:9443/ws/{symbol}@kline_{interval}'

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

running = True
csv_file = 'kline_socket_pepeusdt_validation.csv'

def on_open(ws):
    logger.info('WebSocket connected.')

def on_close(ws, close_status_code, close_msg):
    logger.warning(f'WebSocket connection closed: {close_status_code} - {close_msg}')

def on_error(ws, error):
    logger.error(f'WebSocket error: {error}')

def write_to_csv(data):
    file_exists = os.path.isfile(csv_file)
    
    with open(csv_file, mode='a', newline='') as file:
        writer = csv.writer(file)
        if not file_exists:
            writer.writerow(["date", "time", "open_price", "high_price", "low_price", "close_price", "volume", "number_of_trades", "first_trade_id", "last_trade_id", "quote_asset_volume", "taker_buy_base_asset_volume", "taker_buy_quote_asset_volume", "kline_closed"])
        writer.writerow(data)

def on_message(ws, message):
    global running

    if not running:
        return

    json_message = json.loads(message)
    logger.info(f'Received message: {json_message}')
    kline = json_message['k']
    event_time = json_message['E']
    kline_closed = kline['x']
    open_price = float(kline['o'])
    close_price = float(kline['c'])
    high_price = float(kline['h'])
    low_price = float(kline['l'])
    volume = kline['v']
    number_of_trades = kline['n']
    first_trade_id = kline['f']
    last_trade_id = kline['L']
    quote_asset_volume = kline['q']
    taker_buy_base_asset_volume = kline['V']
    taker_buy_quote_asset_volume = kline['Q']


    log_message = (
        f" \n -Date: {current_date}"
        f" -Time: {event_time}"
        f" -Open Price: {open_price}"
        f" -High Price: {high_price}"
        f" -Low Price: {low_price}"
        f" -Close Price: {close_price}"
        f" -Volume: {volume}"
        f" -number_of_trades: {number_of_trades}"
        f" -candleclosed: {kline_closed}"
    )

    write_to_csv([current_date, event_time, open_price, high_price, low_price, close_price, volume, number_of_trades, first_trade_id, last_trade_id, quote_asset_volume, taker_buy_base_asset_volume, taker_buy_quote_asset_volume, kline_closed])

    logger.info(log_message)

def connect_websocket():
    websocket.enableTrace(False)
    ws = websocket.WebSocketApp(SOCKET,
                                on_open=on_open,
                                on_close=on_close,
                                on_message=on_message,
                                on_error=on_error)
    return ws

ws = connect_websocket()
ws.run_forever()
