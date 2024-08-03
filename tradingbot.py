import websocket
import pandas as pd
import json
import logging
import csv
import os
from datetime import datetime

symbol = 'pepeusdt'
interval = '1m'
current_date = datetime.today().strftime('%d-%m-%y')
rsi_window = 14
close_prices = []
high_prices = []
low_prices = []
average_gain = 0
average_loss = 0
SOCKET = f'wss://stream.binance.com:9443/ws/{symbol}@kline_{interval}'

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

running = True
csv_file = 'kline_socket.csv'

def on_open(ws):
    logger.info('WebSocket connected.')

def on_close(ws, close_status_code, close_msg):
    logger.warning(f'WebSocket connection closed: {close_status_code} - {close_msg}')

def on_error(ws, error):
    logger.error(f'WebSocket error: {error}')

def calculate_rsi():
    global close_prices, average_gain, average_loss

    if len(close_prices) > rsi_window:
        close_prices.pop(0)

    if len(close_prices) < rsi_window:
        return None

    if len(close_prices) == rsi_window:
        gains = [0]
        losses = [0]

        for i in range(1, len(close_prices)):
            price_change = close_prices[i] - close_prices[i-1]
            if price_change > 0:
                gains.append(price_change)
                losses.append(0)
            else:
                gains.append(0)
                losses.append(abs(price_change))

        average_gain = sum(gains) / rsi_window
        average_loss = sum(losses) / rsi_window

    else:
        price_change = close_prices[-1] - close_prices[-2]
        gain = max(price_change, 0)
        loss = abs(min(price_change, 0))

        average_gain = (average_gain * (rsi_window - 1) + gain) / rsi_window
        average_loss = (average_loss * (rsi_window - 1) + loss) / rsi_window

    if average_loss == 0:
        return 100

    rs = average_gain / average_loss
    rsi = 100 - (100 / (1 + rs))

    return rsi

def calculate_pivot_points(high, low, close):
    pivot_point = (high + low + close) / 3
    s1 = 2 * pivot_point - high
    r1 = 2 * pivot_point - low
    s2 = pivot_point - (high - low)
    r2 = pivot_point + (high - low)
    return pivot_point, s1, r1, s2, r2

def write_to_csv(data):
    file_exists = os.path.isfile(csv_file)
    
    with open(csv_file, mode='a', newline='') as file:
        writer = csv.writer(file)
        if not file_exists:
            writer.writerow(["Date", "Time", "Open Price", "Close Price", "High Price", "Low Price", "Volume", "Number of Trades", "RSI", "Pivot Point", "S1", "R1", "S2", "R2"])
        writer.writerow(data)

def on_message(ws, message):
    global running, close_prices, high_prices, low_prices, average_gain, average_loss

    if not running:
        return

    json_message = json.loads(message)
    kline = json_message['k']
    event_time = json_message['E']
    kline_closed = kline['x']
    open_price = float(kline['o'])
    current_price = float(kline['c'])
    high_price = float(kline['h'])
    low_price = float(kline['l'])
    volume = kline['v']
    number_of_trades = kline['n']

    log_message = (
        f" \n -Date: {current_date}"
        f" -Time: {event_time}"
        f" -Open Price: {open_price}"
        f" -Current Price: {current_price}"
        f" -High Price: {high_price}"
        f" -Low Price: {low_price}"
        f" -Close price list: {close_prices}"
        f" -candleclosed: {kline_closed}"
    )

    rsi = calculate_rsi()

    if rsi is not None:
        log_message += f" -RSI: {rsi}"

    if kline_closed:
        close_prices.append(current_price)
        high_prices.append(high_price)
        low_prices.append(low_price)

        if len(close_prices) >= 15:
            high_15m = max(high_prices[-15:])
            low_15m = min(low_prices[-15:])
            close_15m = close_prices[-1]
            pivot_point, s1, r1, s2, r2 = calculate_pivot_points(high_15m, low_15m, close_15m)
        else:
            pivot_point, s1, r1, s2, r2 = None, None, None, None, None

        log_message += (
            f" -Pivot Point: {pivot_point}"
            f" -Support 1 (S1): {s1}"
            f" -Resistance 1 (R1): {r1}"
            f" -Support 2 (S2): {s2}"
            f" -Resistance 2 (R2): {r2}"
        )

        write_to_csv([current_date, event_time, open_price, current_price, high_price, low_price, volume, number_of_trades, rsi, pivot_point, s1, r1, s2, r2])

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
