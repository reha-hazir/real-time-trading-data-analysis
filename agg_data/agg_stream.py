import asyncio
import websockets
import json
from collections import defaultdict
from datetime import datetime

minute_trades = defaultdict(lambda: {"buy": 0, "sell": 0})
current_minute = None

async def binance_websocket():
    global current_minute

    uri = "wss://stream.binance.com:9443/ws/pepeusdt@trade"

    async with websockets.connect(uri) as websocket:
        while True:
            data = await websocket.recv()
            trade = json.loads(data)
            
            trade_time = trade["T"] // 1000
            minute_timestamp = trade_time // 60

            price_btc_usdt = float(trade["p"])

            if trade["m"]:
                minute_trades[minute_timestamp]["buy"] += float(trade["q"])
            else:
                minute_trades[minute_timestamp]["sell"] += float(trade["q"])

            if minute_timestamp != current_minute:
                if current_minute is not None:
                    await print_aggregated_data(current_minute, price_btc_usdt)
                current_minute = minute_timestamp

async def print_aggregated_data(minute_timestamp, price_btc_usdt):
    buy_btc = minute_trades[minute_timestamp]['buy']
    sell_btc = minute_trades[minute_timestamp]['sell']
    
    buy_usdt = buy_btc * price_btc_usdt
    sell_usdt = sell_btc * price_btc_usdt

    print(f"Minute: {datetime.utcfromtimestamp(minute_timestamp * 60)}")
    print(f"   Buy BTC: {buy_btc} (Equivalent USDT: {buy_usdt})")
    print(f"   Sell BTC: {sell_btc} (Equivalent USDT: {sell_usdt})")
    print("-----------------------------------")

if __name__ == "__main__":
    asyncio.get_event_loop().run_until_complete(binance_websocket())
