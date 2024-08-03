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

async def kline_websocket():
    global current_minute, trade_data, last_price

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

                if last_price is not None:
                
                    await print_aggregated_data(current_minute, last_price)

            
                log_message = (
                    f" \n -Date: {current_date}"
                    f" -Time: {event_time}"
                    f" -Open Price: {open_price:.8f}"
                    f" -Current Price: {current_price:.8f}"
                    f" -High Price: {high_price:.8f}"
                    f" -Low Price: {low_price:.8f}"
                    f" -Volume: {volume:.2f}"
                    f" -Trades: {number_of_trades}"
                    f" -Candle Closed: {kline_closed}"
                    f" -Buy: {trade_data['buy']:.2f}"
                    f" -Sell: {trade_data['sell']:.2f}"
                    f" -USDT Buy: {trade_data['buy'] * last_price if last_price else 0:.2f}"
                    f" -USDT Sell: {trade_data['sell'] * last_price if last_price else 0:.2f}"
                    f" -Total Volume: {trade_data['buy'] + trade_data['sell']:.2f}"
                    f" -Number of Trades: {trade_data['count']}"
                )

                # Comparison of buy and sell volumes
                if trade_data['sell'] > trade_data['buy']:
                    log_message += " -Volume Orientation: Sell volume is greater than Buy volume: -1"
                elif trade_data['buy'] > trade_data['sell']:
                    log_message += " -Volume Orientation: Buy volume is greater than Sell volume: 1"
                else:
                    log_message += " -Volume Orientation: Buy volume equals Sell volume: 0"

                logger.info(log_message)

                # Reset trade data for the next kline period
                trade_data = {"buy": 0, "sell": 0, "count": 0, "price_sum": 0, "quantity_sum": 0}
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
                is_sell = trade["m"]  # m: True = sell, False = buy

                if kline_closed_event.is_set():
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

async def print_aggregated_data(minute_timestamp, price_btc_usdt):
    data = trade_data

    buy_volume = data['buy']
    sell_volume = data['sell']
    trade_count = data['count']

    # Calculate average price for the minute
    if data['quantity_sum'] > 0:
        average_price = data['price_sum'] / data['quantity_sum']
    else:
        average_price = 0

    buy_usdt = buy_volume * average_price
    sell_usdt = sell_volume * average_price
    total_volume_btc = buy_volume + sell_volume
    total_volume_usdt = total_volume_btc * average_price

    logger.info(f"Minute: {datetime.utcfromtimestamp(minute_timestamp * 60)}")
    logger.info(f"   Average Price: {average_price:.8f}")
    logger.info(f"   Buy: {buy_volume:.2f} BTC (Equivalent USDT: {buy_usdt:.2f})")
    logger.info(f"   Sell: {sell_volume:.2f} BTC (Equivalent USDT: {sell_usdt:.2f})")
    logger.info(f"   Total Volume: {total_volume_btc:.2f} BTC (Equivalent USDT: {total_volume_usdt:.2f})")
    logger.info(f"   Number of Trades: {trade_count}")

    # Comparison of buy and sell volumes
    if sell_volume > buy_volume:
        logger.info("Sell volume is greater than Buy volume: -1")
    elif buy_volume > sell_volume:
        logger.info("Buy volume is greater than Sell volume: 1")
    else:
        logger.info("Buy volume equals Sell volume: 0")

    logger.info("-----------------------------------")

async def main():
    await asyncio.gather(kline_websocket(), trade_websocket())

if __name__ == "__main__":
    asyncio.run(main())
