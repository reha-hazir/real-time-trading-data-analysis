import asyncio
import websockets
import json
import csv
from datetime import datetime

csv_filename = 'trades_log.csv'
csv_header = ['Timestamp', 'Buy', 'Sell', 'Quantity', 'Price']

async def binance_websocket():
    uri = "wss://stream.binance.com:9443/ws/pepeusdt@trade"

    async with websockets.connect(uri) as websocket:
        with open(csv_filename, mode='w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(csv_header)
            
            while True:
                data = await websocket.recv()
                trade = json.loads(data)
                
                timestamp = datetime.utcfromtimestamp(trade["T"] / 1000).strftime('%Y-%m-%d %H:%M:%S.%f')
                buy = trade["m"]
                sell = not trade["m"]
                quantity = float(trade["q"])
                price = float(trade["p"])

                # Log the trade into CSV
                writer.writerow([timestamp, buy, sell, quantity, price])

if __name__ == "__main__":
    asyncio.get_event_loop().run_until_complete(binance_websocket())
