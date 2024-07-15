import asyncio
import websockets
from collections import defaultdict


minute_trades = defaultdict(lambda: {"buy": 0, "sell": 0})
current_minute = None

async def binance_websocket():
    global current_minute

    uri = "wss://stream.binance.com:9443/ws/pepeusdt@trade"

    async with websockets.connect(uri) as websocket:
        while True:
            data = await websocket.recv()
            
            
            print(data)

if __name__ == "__main__":
    asyncio.get_event_loop().run_until_complete(binance_websocket())
