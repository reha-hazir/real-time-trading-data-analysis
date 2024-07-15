import tradingbot as bot

if __name__ == "__main__":
    ws = bot.connect_websocket()
    ws.run_forever()

    bot.running = False
    ws.close()

    bot.logger.info("WebSocket connection closed.")
