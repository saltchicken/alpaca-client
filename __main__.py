import os
from alpaca.data.live import StockDataStream
from dotenv import load_dotenv

load_dotenv()

class AlpacaStockStreamer:
    """
    A modular class for streaming real-time stock prices from Alpaca via WebSockets.
    Extracted for integration into larger event-driven applications or trading bots.
    """
    def __init__(self, api_key: str = None, secret_key: str = None):
        # Grab credentials from arguments or environment variables
        self.api_key = api_key or os.environ.get("APCA_API_KEY_ID")
        self.secret_key = secret_key or os.environ.get("APCA_API_SECRET_KEY")

        if not self.api_key or not self.secret_key:
            raise ValueError("API Key and Secret Key must be provided.")

        # This manages the websocket connection under the hood.
        self.stream = StockDataStream(self.api_key, self.secret_key)

    async def _trade_callback(self, trade):
        """
        This is the async handler function that gets triggered every time a new trade happens.
        In a real app, you would pass this data to your trading logic or UI.
        """
        print(trade)
        # print(f"[{trade.symbol}] New Trade! Price: ${trade.price:.2f} | Size: {trade.size}")

    def start_streaming(self, symbols: list):
        """
        Subscribes to the specified symbols and starts the blocking websocket connection.
        """
        print(f"Subscribing to trades for: {symbols}")
        
        self.stream.subscribe_trades(self._trade_callback, *symbols)
        
        # You can also subscribe to quotes (bid/ask) instead of executed trades:
        # self.stream.subscribe_quotes(self._quote_callback, *symbols)

        print("Starting stream... (Press Ctrl+C to exit)")
        self.stream.run()

if __name__ == "__main__":
    try:
        streamer = AlpacaStockStreamer()
        symbols_to_watch = ["MSFT"]
        
        # This will block the terminal and print trades as they happen in real-time
        streamer.start_streaming(symbols_to_watch)
        
    except KeyboardInterrupt:
        print("\nStream stopped by user.")
    except Exception as e:
        print(f"An error occurred: {e}")
