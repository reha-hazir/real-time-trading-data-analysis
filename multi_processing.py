import asyncio
import websockets

# Example list of 40 symbols
symbols = ['dogeusdt', 'btcusdt', 'ethusdt', 'xrpusdt', 'ltcusdt', 'linkusdt', 'adausdt', 'dotusdt', 
           'bnbusdt', 'uniusdt', 'bchusdt', 'xlmusdt', 'vetusdt', 'filusdt', 'trxusdt', 'solusdt', 
           'eosusdt', 'xtzusdt', 'aaveusdt', 'sushiusdt', 'atomusdt', 'compusdt', 'dashusdt', 'zecusdt', 
           'snxusdt', 'avaxusdt', 'yfiusdt', 'thetausdt', 'algousdt', 'chzusdt', 'manausdt', 'enjusdt', 
           'batusdt', 'omgusdt', 'iotausdt', 'rvnusdt', 'neousdt', 'icxusdt', 'ontusdt', 'qtumusdt']
intervals = ['1m', '5m', '15m']

async def listen_stream(symbol, interval):
    socket_url = f'wss://stream.binance.com:9443/ws/{symbol}@kline_{interval}'
    async with websockets.connect(socket_url) as websocket:
        while True:
            data = await websocket.recv()
            print(f"Received data for {symbol} at {interval}: {data}")

async def main():
    tasks = [listen_stream(symbol, interval) for symbol in symbols for interval in intervals]
    await asyncio.gather(*tasks)

if __name__ == "__main__":
    asyncio.run(main())
