import json
import threading
import websocket
import numpy as np
import os
import pandas as pd
from datetime import datetime, timedelta
from pytz import timezone
from collections import deque

# Indicator Helper Functions
def calculate_sma(data, window=20):
    """Simple Moving Average."""
    return pd.Series(data).rolling(window=window, min_periods=1).mean()

def calculate_std(data, window=20):
    """Standard Deviation for Bollinger Bands."""
    return pd.Series(data).rolling(window=window, min_periods=1).std()

def calculate_bollinger_bands(data, window=20, num_std=2):
    """Returns upper_band, middle_band, lower_band for Bollinger Bands."""
    sma = calculate_sma(data, window)
    std_dev = calculate_std(data, window)
    upper_band = sma + (num_std * std_dev)
    lower_band = sma - (num_std * std_dev)
    return upper_band, sma, lower_band

def calculate_ema(data, span=12):
    """Exponential Moving Average."""
    return pd.Series(data).ewm(span=span, adjust=False).mean()

def calculate_macd(data, short_span=12, long_span=26, signal_span=9):
    """MACD = EMA(short_span) - EMA(long_span), plus signal line."""
    ema_short = calculate_ema(data, short_span)
    ema_long = calculate_ema(data, long_span)
    macd_line = ema_short - ema_long
    signal_line = macd_line.ewm(span=signal_span, adjust=False).mean()
    return macd_line, signal_line

# Coinbase WebSocket Setup
COINS = ["BTC-USD", "ETH-USD", "XRP-USD", "SOL-USD", "DOGE-USD"]
CB_URL = "wss://ws-feed.exchange.coinbase.com"

# Data Tracking
data_lock = threading.Lock()
trade_data = {coin: [] for coin in COINS}  # Separate trade data for each coin
order_book_data = {coin: [] for coin in COINS}  # Separate order book snapshots for each coin
price_history = {coin: deque(maxlen=26) for coin in COINS}  # To store recent prices for indicators

# Choose Directory for Saving coin data
BASE_DIR = "D:/coin_data_v2"
os.makedirs(BASE_DIR, exist_ok=True)
for coin in COINS:
    # Split coin data between market hours and after hours US
    os.makedirs(os.path.join(BASE_DIR, coin, "normal_trading_hours", "trade_data"), exist_ok=True)
    os.makedirs(os.path.join(BASE_DIR, coin, "normal_trading_hours", "order_book"), exist_ok=True)
    os.makedirs(os.path.join(BASE_DIR, coin, "after_hours", "trade_data"), exist_ok=True)
    os.makedirs(os.path.join(BASE_DIR, coin, "after_hours", "order_book"), exist_ok=True)

last_trade_time = {coin: None for coin in COINS}  # Tracks the last trade time for each coin


# Session Metadata to record start and end time of recorded session
SESSION_START_TIME = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

# Helper functions
def save_numpy_array(data, folder, filename_prefix):
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = os.path.join(folder, f"{filename_prefix}_{timestamp}.npy")
    np.save(filename, data)
    print(f"Saved data to {filename}")

def get_est_now():
    return datetime.now(timezone("US/Eastern"))

# Check if a timestamp is during normal trading hours (9 AM - 4 PM EST).
def is_normal_trading_hours(timestamp): 
    start = timestamp.replace(hour=9, minute=0, second=0, microsecond=0)
    end = timestamp.replace(hour=16, minute=0, second=0, microsecond=0)
    return start <= timestamp <= end

# Save session start/end times to a JSON file.
def save_session_end():
    session_metadata_file = os.path.join(BASE_DIR, "session_metadata.json")
    session_metadata = {
        "start_time": SESSION_START_TIME,
        "end_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }
    with open(session_metadata_file, "w") as f:
        json.dump(session_metadata, f)
    print(f"Session metadata saved to {session_metadata_file}")

def save_data_periodically():
    while True:
        threading.Event().wait(1800)  # Wait 30 minutes between each data save
        with data_lock:
            for coin in COINS:
                if not trade_data[coin] and not order_book_data[coin]:
                    continue

                est_now = get_est_now()
                folder = "normal_trading_hours" if is_normal_trading_hours(est_now) else "after_hours"

                # Normal trade data with indicators
                if trade_data[coin]:
                    trade_folder = os.path.join(BASE_DIR, coin, folder, "trade_data")
                    trade_array = np.array(trade_data[coin], dtype=object)
                    save_numpy_array(trade_array, trade_folder, "trade_data")
                    trade_data[coin].clear()

                # Book snapshot with timestamps
                if order_book_data[coin]:
                    order_book_folder = os.path.join(BASE_DIR, coin, folder, "order_book")
                    order_book_array = np.array(order_book_data[coin], dtype=object)
                    save_numpy_array(order_book_array, order_book_folder, "order_book")
                    order_book_data[coin].clear()


# WebSocket functions
def on_message(ws, message):
    try:
        # Check if message is a string before parsing
        if isinstance(message, bytes):
            message = message.decode('utf-8')

        # Attempt to parse the message as JSON
        data = json.loads(message)

       
        if 'type' not in data:
            return

        product_id = data.get("product_id")

        if product_id not in COINS:
            return

        # Process trade data
        if data.get("type") == "match":
            price = float(data["price"])
            time_str = data.get("time")

            if time_str:
                timestamp = pd.to_datetime(time_str).to_pydatetime()
            else:
                timestamp = datetime.utcnow()

            # Calculate time difference since last trade
            with data_lock:
                time_diff = (timestamp - last_trade_time[product_id]).total_seconds() if last_trade_time[product_id] else 0
                last_trade_time[product_id] = timestamp

                # Update price history
                price_history[product_id].append(price)

                # Initialize bonus indicators
                upper_bb = lower_bb = macd = signal = np.nan

                prices = list(price_history[product_id])

                # Calculate Bollinger Bands if enough data
                if len(prices) >= 20:
                    upper, middle, lower = calculate_bollinger_bands(prices, window=20, num_std=2)
                    upper_bb = upper.iloc[-1]
                    lower_bb = lower.iloc[-1]

                # Calculate MACD/Signal if enough data
                if len(prices) >= 26:
                    macd_line, signal_line = calculate_macd(prices, short_span=12, long_span=26, signal_span=9)
                    macd = macd_line.iloc[-1]
                    signal = signal_line.iloc[-1]

                # Append trade data with indicators for saving later
                trade_data[product_id].append([timestamp, price, time_diff, upper_bb, lower_bb, macd, signal])

        # Get book snapshot
        elif data.get("type") == "snapshot":
            bids = data.get("bids", [])  # List of [price, size]
            asks = data.get("asks", [])  # List of [price, size]
            timestamp = datetime.utcnow()

            with data_lock:
                order_book_data[product_id].append([timestamp, bids, asks])

    except json.JSONDecodeError:
        # NOT valid JSON
        print("Error in on_message: Received non-JSON message.")
    except Exception as e:
        # Error handling
        print(f"Error in on_message: {type(e).__name__} - {e}")

def on_error(ws, error):
    print(f"WebSocket error: {error}")

def on_open(ws):
    print("WebSocket connection opened.")
    subscribe_message = {
        "type": "subscribe",
        "product_ids": COINS,
        "channels": ["matches", "level2"]  # Trades and order book updates
    }
    ws.send(json.dumps(subscribe_message))

def on_close(ws, close_status_code, close_msg):
    print(f"WebSocket closed: {close_msg} (Code: {close_status_code})")
    print("Attempting to reconnect in 0.2 seconds...")
    threading.Timer(0.2, start_websocket).start()

def start_websocket():
    ws = websocket.WebSocketApp(
        CB_URL,
        on_message=on_message,
        on_error=on_error,
        on_open=on_open,
        on_close=on_close
    )
    # Sometimes when sending q request, websocket will give an error with an unknown reason. 
    # If this happens, we just try again.
    ws.run_forever(ping_interval=60, ping_timeout=10)


# Main
def main():
    # Start the WebSocket listener in a background thread
    ws_thread = threading.Thread(target=start_websocket, daemon=True)
    ws_thread.start()

    # Start the periodic saving thread
    save_thread = threading.Thread(target=save_data_periodically, daemon=True)
    save_thread.start()

    # Keep the main thread running
    try:
        while True:
            threading.Event().wait(1)
    except KeyboardInterrupt:
        print("Exiting program.")
        save_session_end()

if __name__ == "__main__":
    main()
