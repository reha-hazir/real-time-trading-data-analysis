import asyncio
import websockets
import json
from collections import defaultdict
from datetime import datetime

minute_trades = defaultdict(lambda: {"buy": 0, "sell": 0})
current_minute = None  # Initialize outside the function

async def binance_websocket():
    global current_minute  # Use global keyword to modify the global variable

    uri = "wss://stream.binance.com:9443/ws/btcusdt@trade"

    async with websockets.connect(uri) as websocket:
        while True:
            data = await websocket.recv()
            trade = json.loads(data)
            
            # Convert trade time to minute-based timestamp
            trade_time = trade["T"] // 1000  # Convert milliseconds to seconds
            minute_timestamp = trade_time // 60  # Convert to minute-based timestamp

            # Determine if it's a buy or sell and accumulate
            if trade["m"]:
                minute_trades[minute_timestamp]["buy"] += float(trade["q"])
            else:
                minute_trades[minute_timestamp]["sell"] += float(trade["q"])

            # Check if a new minute has started
            if minute_timestamp != current_minute:
                current_minute = minute_timestamp
                
                # Print aggregated data for the current minute
                print(f"Minute: {datetime.utcfromtimestamp(minute_timestamp * 60)}")
                print(f"   Buy BTC: {minute_trades[minute_timestamp]['buy']}")
                print(f"   Sell BTC: {minute_trades[minute_timestamp]['sell']}")
                print("-----------------------------------")

if __name__ == "__main__":
    asyncio.get_event_loop().run_until_complete(binance_websocket())
