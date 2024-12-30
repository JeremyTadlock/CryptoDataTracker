import sys
import json
import threading
import websocket
import numpy as np
import os
import pandas as pd
from datetime import datetime, timedelta
from pytz import timezone


# Coinbase WebSocket Setup
COINS = ["BTC-USD", "ETH-USD", "XRP-USD", "SOL-USD", "DOGE-USD"]
CB_URL = "wss://ws-feed.exchange.coinbase.com"

# Data Tracking
data_lock = threading.Lock()
trade_data = {coin: [] for coin in COINS}  # Separate trade data for each coin
order_book_data = {coin: [] for coin in COINS}  # Separate order book snapshots for each coin

# Choose directory for saving coin data
BASE_DIR = "D:/coin_data"
os.makedirs(BASE_DIR, exist_ok=True)
for coin in COINS:
    # Split coin data between market hours and after hours US
    os.makedirs(os.path.join(BASE_DIR, coin, "normal_trading_hours", "trade_data"), exist_ok=True)
    os.makedirs(os.path.join(BASE_DIR, coin, "normal_trading_hours", "order_book"), exist_ok=True)
    os.makedirs(os.path.join(BASE_DIR, coin, "after_hours", "trade_data"), exist_ok=True)
    os.makedirs(os.path.join(BASE_DIR, coin, "after_hours", "order_book"), exist_ok=True)

last_trade_time = {coin: None for coin in COINS}  # Tracks the last trade time for each coin

# Helper Functions
def save_numpy_array(data, folder, filename_prefix):
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = os.path.join(folder, f"{filename_prefix}_{timestamp}.npy")
    np.save(filename, data)
    print(f"[INFO] Saved data to {filename}")

def get_est_now():
    return datetime.now(timezone("US/Eastern"))

def is_normal_trading_hours(timestamp):  # Check if a timestamp is during normal trading hours (9 AM - 4 PM EST)
    start = timestamp.replace(hour=9, minute=0, second=0, microsecond=0)
    end = timestamp.replace(hour=16, minute=0, second=0, microsecond=0)
    return start <= timestamp <= end

def save_data_periodically():
    while True:
        threading.Event().wait(1800)  # Wait 30 minutes between each data save
        with data_lock:
            for coin in COINS:
                if not trade_data[coin] and not order_book_data[coin]:
                    continue

                est_now = get_est_now()
                folder = "normal_trading_hours" if is_normal_trading_hours(est_now) else "after_hours"

                # Normal trade data with timestamps
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
        data = json.loads(message)
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

                trade_data[product_id].append([timestamp, price, time_diff])

        # Get book snapshot
        elif data.get("type") == "snapshot":
            bids = data.get("bids", [])  # List of [price, size]
            asks = data.get("asks", [])  # List of [price, size]
            timestamp = datetime.utcnow()

            with data_lock:
                order_book_data[product_id].append([timestamp, bids, asks])

    except Exception as e:
        print("Error in on_message:", e)

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
    print("WebSocket closed:", close_msg)
    print("Attempting to reconnect in 2 seconds...")
    threading.Timer(2, start_websocket).start()

def start_websocket():
    ws = websocket.WebSocketApp(
        CB_URL,
        on_message=on_message,
        on_error=on_error,
        on_open=on_open,
        on_close=on_close
    )
    ws.run_forever()

# save json marker showing data saving session ended
def on_exit():
    with open(os.path.join(BASE_DIR, "session_end.json"), "w") as f:
        json.dump({"end_time": datetime.now().strftime("%Y%m%d_%H%M%S")}, f)

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

if __name__ == "__main__":
    main()
