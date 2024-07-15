import websocket
import pandas as pd
import json
import logging
from datetime import datetime

symbol = 'dogeusdt'
interval = '1m'
current_date = datetime.today().strftime('%d-%m-%y')
rsi_window = 2
close_prices = []
average_gain = 0
average_loss = 0
SOCKET = f'wss://stream.binance.com:9443/ws/{symbol}@kline_{interval}'

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

running = True

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

def on_message(ws, message):
    global running, close_prices, average_gain, average_loss

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

    logger.info(log_message)

    if kline_closed:
        close_prices.append(current_price)

def connect_websocket():
    websocket.enableTrace(False)
    ws = websocket.WebSocketApp(SOCKET,
                                on_open=on_open,
                                on_close=on_close,
                                on_message=on_message,
                                on_error=on_error)
    return ws
