import asyncio
from websocket_client import WebSocketHandler
from database_handler import DatabaseHandler

symbol = 'btcusdt'
interval = '1m'
table_name = symbol + '_' + interval + '_ALONE'

db_handler = DatabaseHandler(table_name)

async def kline_callback(data):
    db_handler.insert_row(data)

async def main():
    ws_handler = WebSocketHandler(symbol, interval)
    
   
    await ws_handler.wait_for_initial_kline_closed()
    
    await asyncio.gather(
        ws_handler.kline_websocket(kline_callback),
        ws_handler.trade_websocket()
    )

if __name__ == "__main__":
    asyncio.run(main())
