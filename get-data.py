import requests
import csv
from datetime import datetime
import os
import time

# Global interval counter
interval_counter = 0

def ensure_data_dir():
    data_dir = './data'
    if not os.path.exists(data_dir):
        os.makedirs(data_dir)
    return data_dir

def fetch_price_data(symbol='BTCUSDT'):
    url = f'https://api.binance.com/api/v3/ticker/price?symbol={symbol}'
    
    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching price data: {e}")
        return None

def write_price_data(price_data, interval):
    data_dir = ensure_data_dir()
    filepath = os.path.join(data_dir, 'prices.csv')
    
    with open(filepath, 'a', newline='') as csvfile:
        writer = csv.writer(csvfile)
        if os.path.getsize(filepath) == 0:
            writer.writerow(['interval', 'symbol', 'price'])
        writer.writerow([
            interval,
            price_data['symbol'],
            price_data['price']
        ])

def fetch_orderbook_data(symbol='BTCUSDT', limit=100):
    url = f'https://www.binance.com/fapi/v1/depth?symbol={symbol}&limit={limit}'
    
    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching orderbook data: {e}")
        return None

def fetch_trades_data(symbol='BTCUSDT', limit=100):
    url = f'https://www.binance.com/fapi/v1/trades?symbol={symbol}&limit={limit}'
    
    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching trades data: {e}")
        return None

def calculate_orderbook_metrics(bids, asks):
    # Get highest bid and lowest ask
    highest_bid = float(bids[0][0])  # First bid price
    lowest_ask = float(asks[0][0])   # First ask price
    
    # Calculate total volumes
    total_bid_volume = sum(float(bid[1]) for bid in bids)
    total_ask_volume = sum(float(ask[1]) for ask in asks)
    
    # Calculate VWAP for bids and asks separately
    bid_vwap = sum(float(bid[0]) * float(bid[1]) for bid in bids) / total_bid_volume if total_bid_volume > 0 else 0
    ask_vwap = sum(float(ask[0]) * float(ask[1]) for ask in asks) / total_ask_volume if total_ask_volume > 0 else 0
    
    # Calculate spread
    spread = lowest_ask - highest_bid
    
    # Calculate bid/ask ratio
    bid_ask_ratio = total_bid_volume / total_ask_volume if total_ask_volume > 0 else 0
    
    return {
        'total_bid_volume': round(total_bid_volume, 8),
        'total_ask_volume': round(total_ask_volume, 8),
        'spread': round(spread, 8),
        'bid_ask_ratio': round(bid_ask_ratio, 8),
        'pressure': "buy" if bid_ask_ratio > 1 else "sell",
        'bid_vwap': round(bid_vwap, 8),
        'ask_vwap': round(ask_vwap, 8)
    }

def get_last_intervals_pressure(number):
    data_dir = ensure_data_dir()
    metrics_file = os.path.join(data_dir, 'metrics.csv')
    
    try:
        with open(metrics_file, 'r') as csvfile:
            # Read all rows
            reader = csv.DictReader(csvfile)
            rows = list(reader)
            
            # Get last n intervals' pressure
            if len(rows) >= number:
                last_n_pressure = [row['pressure'] for row in rows[-number:]]
                
                # Check if all are the same
                if all(pressure == 'buy' for pressure in last_n_pressure):
                    return 'buy'
                elif all(pressure == 'sell' for pressure in last_n_pressure):
                    return 'sell'
            
            return 'neutral'  # Default if not enough data or no clear trend
            
    except Exception as e:
        print(f"Error reading metrics file: {e}")
        return 'neutral'

def get_last_bid_ask_ratio_change(number):
    data_dir = ensure_data_dir()
    metrics_file = os.path.join(data_dir, 'metrics.csv')
    
    try:
        with open(metrics_file, 'r') as csvfile:
            # Read all rows
            reader = csv.DictReader(csvfile)
            rows = list(reader)
            
            # Get last n intervals' bid_ask_ratio
            if len(rows) >= number:
                last_n_ratios = [float(row['bid_ask_ratio']) for row in rows[-number:]]
                
                # Get min and max from all ratios
                min_ratio = min(last_n_ratios)
                max_ratio = max(last_n_ratios)
                
                # Calculate percentage change
                if min_ratio > 0:  # Avoid division by zero
                    change_percentage = ((max_ratio - min_ratio) / min_ratio) * 100
                    return round(change_percentage, 8)
            
            return 0  # Default if not enough data
            
    except Exception as e:
        print(f"Error reading metrics file: {e}")
        return 0

def get_last_vwap_price_direction(number):
    data_dir = ensure_data_dir()
    metrics_file = os.path.join(data_dir, 'metrics.csv')
    
    try:
        with open(metrics_file, 'r') as csvfile:
            # Read all rows
            reader = csv.DictReader(csvfile)
            rows = list(reader)
            
            # Get last n intervals' VWAP (using average of bid and ask VWAP)
            if len(rows) >= number:
                last_n_vwap = [(float(row['bid_vwap']) + float(row['ask_vwap'])) / 2 for row in rows[-number:]]
                
                # Calculate current price (using latest VWAP)
                current_price = last_n_vwap[-1]  # Use latest VWAP as reference
                
                # Check if all VWAPs are consistently above or below current price
                if all(vwap > current_price for vwap in last_n_vwap):
                    return 'above'
                elif all(vwap < current_price for vwap in last_n_vwap):
                    return 'below'
            
            return 'neutral'  # Default if no clear direction
            
    except Exception as e:
        print(f"Error reading metrics file: {e}")
        return 'neutral'

def write_prediction_data(interval, bid_vwap, ask_vwap, current_price):
    data_dir = ensure_data_dir()
    filepath = os.path.join(data_dir, 'predictions.csv')
    
    forecast = (bid_vwap + ask_vwap) / 2 if (bid_vwap > 0 and ask_vwap > 0) else 0
    difference = forecast - float(current_price)
    
    with open(filepath, 'a', newline='') as csvfile:
        writer = csv.writer(csvfile)
        if os.path.getsize(filepath) == 0:
            writer.writerow(['interval', 'current_price', 'forecast', 'difference'])
        writer.writerow([
            interval,
            current_price,
            round(forecast, 8),
            round(difference, 8)
        ])

def write_trends_data(interval):
    data_dir = ensure_data_dir()
    filepath = os.path.join(data_dir, 'trends.csv')
    
    # Get trends for different intervals
    last_3_pressure = get_last_intervals_pressure(3)
    last_3_ratio_change = get_last_bid_ask_ratio_change(3)
    last_3_vwap_direction = get_last_vwap_price_direction(3)
    
    # Append mode
    with open(filepath, 'a', newline='') as csvfile:
        writer = csv.writer(csvfile)
        # Write header if file is empty
        if os.path.getsize(filepath) == 0:
            writer.writerow(['interval', 'last_3_intervals_pressure', 
                           'last_3_bid_ask_ratio_change', 'last_3_vwap_price_direction'])
        writer.writerow([interval, last_3_pressure, last_3_ratio_change, last_3_vwap_direction])

def write_metrics_data(metrics, interval):
    data_dir = ensure_data_dir()
    filepath = os.path.join(data_dir, 'metrics.csv')
    
    # Append mode
    with open(filepath, 'a', newline='') as csvfile:
        writer = csv.writer(csvfile)
        # Write header if file is empty
        if os.path.getsize(filepath) == 0:
            writer.writerow(['interval', 'total_bid_volume', 'total_ask_volume', 
                           'spread', 'bid_ask_ratio', 'pressure', 'bid_vwap', 'ask_vwap'])
        writer.writerow([
            interval,
            metrics['total_bid_volume'],
            metrics['total_ask_volume'],
            metrics['spread'],
            metrics['bid_ask_ratio'],
            metrics['pressure'],
            metrics['bid_vwap'],
            metrics['ask_vwap']
        ])

def write_to_csv(data, filename, time, interval):
    data_dir = ensure_data_dir()
    filepath = os.path.join(data_dir, filename)
    
    with open(filepath, 'a', newline='') as csvfile:
        writer = csv.writer(csvfile)
        if os.path.getsize(filepath) == 0:
            writer.writerow(['interval', 'time', 'price', 'quantity'])
        for price, quantity in data:
            writer.writerow([interval, time, price, quantity])

def write_trades_to_csv(trades_data, filename, interval):
    data_dir = ensure_data_dir()
    filepath = os.path.join(data_dir, filename)
    
    with open(filepath, 'a', newline='') as csvfile:
        writer = csv.writer(csvfile)
        if os.path.getsize(filepath) == 0:
            writer.writerow(['interval', 'time', 'price', 'quantity'])
        for trade in trades_data:
            writer.writerow([
                interval,
                trade['time'],
                trade['price'],
                trade['qty']
            ])

def create_empty_files():
    data_dir = ensure_data_dir()
    files = {
        'bids.csv': ['interval', 'time', 'price', 'quantity'],
        'asks.csv': ['interval', 'time', 'price', 'quantity'],
        'trades.csv': ['interval', 'time', 'price', 'quantity'],
        'metrics.csv': ['interval', 'total_bid_volume', 'total_ask_volume', 
                       'spread', 'bid_ask_ratio', 'pressure', 'bid_vwap', 'ask_vwap'],
        'trends.csv': ['interval', 'last_3_intervals_pressure', 
                      'last_3_bid_ask_ratio_change', 'last_3_vwap_price_direction'],
        'prices.csv': ['interval', 'symbol', 'price'],
        'predictions.csv': ['interval', 'current_price', 'forecast', 'difference']
    }
    
    for filename, headers in files.items():
        filepath = os.path.join(data_dir, filename)
        if not os.path.exists(filepath):
            with open(filepath, 'w', newline='') as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow(headers)

def fetch_and_write_data():
    global interval_counter
    interval_counter += 1
    
    print(f"\nFetching data for interval {interval_counter} at {datetime.now()}")
    
    # Fetch and write price data
    price_data = fetch_price_data()
    current_price = None
    if price_data:
        current_price = price_data['price']
        write_price_data(price_data, interval_counter)
        print(f"Price data written for interval {interval_counter}")
    else:
        print("Failed to fetch price data")
    
    # Fetch and write orderbook data
    orderbook_data = fetch_orderbook_data()
    if orderbook_data:
        time_val = orderbook_data['T']
        bids = orderbook_data['bids']
        asks = orderbook_data['asks']
        
        # Write orderbook data
        write_to_csv(bids, 'bids.csv', time_val, interval_counter)
        write_to_csv(asks, 'asks.csv', time_val, interval_counter)
        print(f"Orderbook data written for interval {interval_counter}")
        
        # Calculate and write metrics data
        metrics = calculate_orderbook_metrics(bids, asks)
        write_metrics_data(metrics, interval_counter)
        print(f"Metrics data written for interval {interval_counter}")
        
        # Write prediction data if we have current price
        if current_price is not None:
            write_prediction_data(interval_counter, metrics['bid_vwap'], metrics['ask_vwap'], current_price)
            print(f"Prediction data written for interval {interval_counter}")
        
        # Write trends data
        write_trends_data(interval_counter)
        print(f"Trends data written for interval {interval_counter}")
    else:
        print("Failed to fetch orderbook data")

    # Fetch and write trades data
    trades_data = fetch_trades_data()
    if trades_data:
        write_trades_to_csv(trades_data, 'trades.csv', interval_counter)
        print(f"Trades data written for interval {interval_counter}")
    else:
        print("Failed to fetch trades data")

def main():
    create_empty_files()
    
    print("Starting data collection every 10 seconds. Press CTRL+C to stop.")
    
    try:
        while True:
            fetch_and_write_data()
            time.sleep(10)
            
    except KeyboardInterrupt:
        print("\nData collection stopped by user")
    except Exception as e:
        print(f"\nAn error occurred: {e}")


if __name__ == "__main__":
   main()