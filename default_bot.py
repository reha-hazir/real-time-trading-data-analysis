import asyncio
import websockets
import json
import logging
from datetime import datetime

symbol = 'pepeusdt'
interval = '1m'
current_date = datetime.today().strftime('%d-%m-%y')

KLINE_SOCKET = f'wss://stream.binance.com:9443/ws/{symbol}@kline_{interval}'
TRADE_SOCKET = f'wss://stream.binance.com:9443/ws/{symbol}@trade'

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

trade_data = {"buy": 0, "sell": 0, "count": 0, "price_sum": 0, "quantity_sum": 0}
current_minute = None
last_price = None
kline_closed_event = asyncio.Event()
data_lock = asyncio.Lock()  

async def kline_websocket():
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

                log_message = (
                    f"\n -Date: {current_date}"
                    f" -Time: {event_time}"
                    f" -Open Price: {open_price:.8f}"
                    f" -Close Price: {current_price:.8f}"
                    f" -High Price: {high_price:.8f}"
                    f" -Low Price: {low_price:.8f}"
                    f" -Volume: {volume:.2f}"
                    f" -Trades: {number_of_trades}"
                    f" -Buy: {buy_volume:.2f}"
                    f" -Sell: {sell_volume:.2f}"
                    f" -USDT Buy: {buy_usdt:.2f}"
                    f" -USDT Sell: {sell_usdt:.2f}"
                    f" -Total Volume: {total_volume_btc:.2f}"
                    f" -Number of Trades: {trade_count}"
                    f" -Average Price: {average_price:.8f}"
                    f" -Volume Dominance: {volume_dominance}"
                )

                logger.info(log_message)

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
    await asyncio.gather(kline_websocket(), trade_websocket())

if __name__ == "__main__":
    asyncio.run(main())
