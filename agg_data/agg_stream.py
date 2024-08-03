import asyncio
import websockets
import json
from collections import defaultdict
from datetime import datetime

minute_trades = defaultdict(lambda: {"buy": 0, "sell": 0, "count": 0})
current_minute = None
last_price = None

async def binance_websocket():
    global current_minute, last_price

    uri = "wss://stream.binance.com:9443/ws/btcusdt@trade"

    async with websockets.connect(uri) as websocket:
        try:
            async for data in websocket:
                trade = json.loads(data)

                trade_time = trade["T"] // 1000
                price_btc_usdt = float(trade["p"])
                quantity_btc = float(trade["q"])

                minute_timestamp = trade_time // 60

                if current_minute is None:
                    current_minute = minute_timestamp

                if minute_timestamp != current_minute:
                    
                    await print_aggregated_data(current_minute, last_price)

                    
                    current_minute = minute_timestamp

                
                last_price = price_btc_usdt

                
                if trade["m"]:
                    minute_trades[current_minute]["sell"] += quantity_btc
                else:
                    minute_trades[current_minute]["buy"] += quantity_btc

                minute_trades[current_minute]["count"] += 1

        except (websockets.ConnectionClosedError, Exception) as e:
            print(f"Connection closed or error: {e}")
        finally:
            if current_minute is not None:
                await print_aggregated_data(current_minute, last_price)

async def print_aggregated_data(minute_timestamp, price_btc_usdt):
    buy_btc = minute_trades[minute_timestamp]['buy']
    sell_btc = minute_trades[minute_timestamp]['sell']
    trade_count = minute_trades[minute_timestamp]['count']
    
    buy_usdt = buy_btc * price_btc_usdt
    sell_usdt = sell_btc * price_btc_usdt
    total_volume_btc = buy_btc + sell_btc
    total_volume_usdt = total_volume_btc * price_btc_usdt

    print(f"Minute: {datetime.utcfromtimestamp(minute_timestamp * 60)}")
    print(f"Minute unix: {minute_timestamp * 60}")
    print(f"   Buy: {buy_btc} BTC (Equivalent USDT: {buy_usdt})")
    print(f"   Sell: {sell_btc} BTC (Equivalent USDT: {sell_usdt})")
    print(f"   Total Volume: {total_volume_btc} BTC (Equivalent USDT: {total_volume_usdt})")
    print(f"   Number of Trades: {trade_count}")

    # Comparison of buy and sell volumes
    if sell_btc > buy_btc:
        print("Sell volume is greater than Buy volume: -1")
    elif buy_btc > sell_btc:
        print("Buy volume is greater than Sell volume: 1")
    else:
        print("Buy volume equals Sell volume: 0")

    print("-----------------------------------")

if __name__ == "__main__":
    asyncio.run(binance_websocket())
