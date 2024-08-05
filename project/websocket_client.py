import asyncio
import websockets
import json
import logging
from datetime import datetime

# Correct the logging configuration
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')

logger = logging.getLogger(__name__)

KLINE_SOCKET_TEMPLATE = 'wss://stream.binance.com:9443/ws/{}@kline_{}'
TRADE_SOCKET_TEMPLATE = 'wss://stream.binance.com:9443/ws/{}@trade'

class WebSocketHandler:
    def __init__(self, symbol, interval):
        self.symbol = symbol
        self.interval = interval
        self.kline_socket = KLINE_SOCKET_TEMPLATE.format(symbol, interval)
        self.trade_socket = TRADE_SOCKET_TEMPLATE.format(symbol)
        self.trade_data = {"buy": 0, "sell": 0, "count": 0, "price_sum": 0, "quantity_sum": 0}
        self.current_minute = None
        self.last_price = None
        self.data_lock = asyncio.Lock()
        self.kline_closed_event = asyncio.Event()
        self.initialized = False

    async def wait_for_initial_kline_closed(self):
        async with websockets.connect(self.kline_socket) as websocket:
            async for message in websocket:
                json_message = json.loads(message)
                kline = json_message['k']
                kline_closed = kline['x']

                if kline_closed:
                    self.initialized = True
                    break

    async def kline_websocket(self, callback):
        async with websockets.connect(self.kline_socket) as websocket:
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
                    self.current_minute = event_time // 1000 // 60

                    async with self.data_lock:
                        buy_volume = self.trade_data['buy']
                        sell_volume = self.trade_data['sell']
                        trade_count = self.trade_data['count']
                        average_price = (self.trade_data['price_sum'] / self.trade_data['quantity_sum']) if self.trade_data['quantity_sum'] > 0 else 0

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

                    data = {
                        "date": datetime.today().strftime('%d-%m-%y'),
                        "time": event_time,
                        "open_price": open_price,
                        "current_price": current_price,
                        "high_price": high_price,
                        "low_price": low_price,
                        "volume": volume,
                        "number_of_trades": number_of_trades,
                        "buy_volume": buy_volume,
                        "sell_volume": sell_volume,
                        "usdt_buy": buy_usdt,
                        "usdt_sell": sell_usdt,
                        "total_volume": total_volume_btc,
                        "trade_count": trade_count,
                        "average_price": average_price,
                        "volume_dominance": volume_dominance
                    }

                    await callback(data)

                    async with self.data_lock:
                        self.trade_data = {"buy": 0, "sell": 0, "count": 0, "price_sum": 0, "quantity_sum": 0}
                    
                    self.kline_closed_event.set()

    async def trade_websocket(self):
        async with websockets.connect(self.trade_socket) as websocket:
            try:
                async for message in websocket:
                    trade = json.loads(message)

                    trade_time = trade["T"]
                    price_usdt = float(trade["p"])
                    quantity = float(trade["q"])
                    is_sell = trade["m"]
                    
                    async with self.data_lock:
                        if is_sell:
                            self.trade_data["sell"] += quantity
                        else:
                            self.trade_data["buy"] += quantity

                        self.trade_data["count"] += 1
                        self.trade_data["price_sum"] += price_usdt * quantity
                        self.trade_data["quantity_sum"] += quantity
                        self.last_price = price_usdt

            except (websockets.ConnectionClosedError, Exception) as e:
                logger.error(f"Connection closed or error: {e}")
