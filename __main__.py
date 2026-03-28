import os
import asyncpg
from alpaca.data.live import StockDataStream
from dotenv import load_dotenv

load_dotenv()


class DatabaseManager:
    """
    Manages asynchronous connections and queries to a PostgreSQL database.
    Designed to be injected into streaming components.
    """
    def __init__(self, dsn: str):
        self.dsn = dsn
        self.pool = None

    async def connect_if_needed(self):
        """
        Lazily initializes the connection pool from within the running event loop.
        This prevents asyncio loop-attachment errors when combining with Alpaca's run().
        """
        if not self.pool:
            self.pool = await asyncpg.create_pool(self.dsn)
            

            async with self.pool.acquire() as conn:
                await conn.execute('''
                    CREATE TABLE IF NOT EXISTS current_stock_prices (
                        symbol VARCHAR(10) PRIMARY KEY,
                        price NUMERIC(10, 4),
                        last_updated TIMESTAMPTZ
                    )
                ''')

    async def upsert_price(self, symbol: str, price: float, timestamp):
        """
        Updates the stock price. If the symbol doesn't exist, it inserts it.
        If it does exist, it updates the price and timestamp to keep it current.
        """
        await self.connect_if_needed()
        async with self.pool.acquire() as conn:
            await conn.execute('''
                INSERT INTO current_stock_prices (symbol, price, last_updated)
                VALUES ($1, $2, $3)
                ON CONFLICT (symbol) DO UPDATE
                SET price = EXCLUDED.price,
                    last_updated = EXCLUDED.last_updated
            ''', symbol, price, timestamp)


class AlpacaStockStreamer:
    """
    A modular class for streaming real-time stock prices from Alpaca via WebSockets.
    Extracted for integration into larger event-driven applications or trading bots.
    """

    def __init__(self, db_manager: DatabaseManager, api_key: str = None, secret_key: str = None):
        # Grab credentials from arguments or environment variables
        self.api_key = api_key or os.environ.get("APCA_API_KEY_ID")
        self.secret_key = secret_key or os.environ.get("APCA_API_SECRET_KEY")
        self.db = db_manager

        if not self.api_key or not self.secret_key:
            raise ValueError("API Key and Secret Key must be provided.")

        # This manages the websocket connection under the hood.
        self.stream = StockDataStream(self.api_key, self.secret_key)

    async def _trade_callback(self, trade):
        """
        This is the async handler function that gets triggered every time a new trade happens.
        """
        print(f"[{trade.symbol}] New Trade! Price: ${trade.price:.2f} | Time: {trade.timestamp}")
        

        try:
            await self.db.upsert_price(
                symbol=trade.symbol, 
                price=trade.price, 
                timestamp=trade.timestamp
            )
        except Exception as e:
            print(f"Database error for {trade.symbol}: {e}")

    def start_streaming(self, symbols: list):
        """
        Subscribes to the specified symbols and starts the blocking websocket connection.
        """
        print(f"Subscribing to trades for: {symbols}")
        self.stream.subscribe_trades(self._trade_callback, *symbols)
        
        print("Starting stream... (Press Ctrl+C to exit)")
        self.stream.run()


if __name__ == "__main__":
    try:

        db_url = os.environ.get("DATABASE_URL")
        if not db_url:
             raise ValueError("DATABASE_URL environment variable is missing.")


        db_manager = DatabaseManager(dsn=db_url)
        

        streamer = AlpacaStockStreamer(db_manager=db_manager)
        symbols_to_watch = ["MSFT", "AAPL"]
        
        # This will block the terminal and print trades as they happen in real-time
        streamer.start_streaming(symbols_to_watch)
        
    except KeyboardInterrupt:
        print("\nStream stopped by user.")
    except Exception as e:
        print(f"An error occurred: {e}")
