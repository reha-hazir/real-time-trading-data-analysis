import asyncio
import websockets
import json

async def binance_websocket():
    uri = "wss://stream.binance.com:9443/ws/btcusdt@trade"

    async with websockets.connect(uri) as websocket:
        while True:
            data = await websocket.recv()
            trade = json.loads(data)
            print(trade)  # Print the trade data or process it further

if __name__ == "__main__":
    asyncio.get_event_loop().run_until_complete(binance_websocket())
