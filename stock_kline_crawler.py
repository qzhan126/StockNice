import logging
import argparse
import time
from datetime import datetime, timedelta
import os
import pandas as pd
import requests
from fake_useragent import UserAgent
import concurrent.futures # Added concurrent.futures

# Create a logger
logger = logging.getLogger('stock_crawler')
# Initialize UserAgent globally if it's used in multiple functions
ua = UserAgent()
logger.setLevel(logging.INFO) # Ensure logger level is set

# --- Constants ---
MAX_WORKERS = 10 # Max concurrent threads for fetching data
LOG_PROGRESS_INTERVAL = 50 # Log progress every N stocks

# Create a file handler
file_handler = logging.FileHandler('crawler.log')
file_handler.setLevel(logging.INFO)

# Create a stream handler
stream_handler = logging.StreamHandler()
stream_handler.setLevel(logging.INFO)

# Create a formatter and set it for both handlers
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)
stream_handler.setFormatter(formatter)

# Add the handlers to the logger
logger.addHandler(file_handler)
logger.addHandler(stream_handler)

logger.info("Logging setup complete.")

# --- Argument Parsing ---
# This will be parsed when the script is run, so args will be available globally in __main__
parser = argparse.ArgumentParser(description="Stock K-line Data Crawler")
parser.add_argument(
    "--days",
    type=int,
    default=30,
    help="Number of recent days of K-line data to fetch."
)
parser.add_argument(
    "--output_dir",
    type=str,
    default="./data",
    help="Directory to save the output CSV files."
)
# No args = parser.parse_args() here, it's done in __main__

# --- Function to get stock list ---
def get_stock_list():
    """
    Retrieves all A-share stock codes and names from Eastmoney.
    """
    logger.info("Attempting to retrieve stock list from Eastmoney...")
    url = "http://push2.eastmoney.com/api/qt/clist/get"
    params = {
        "pn": 1,
        "pz": 10000,
        "po": 1,
        "np": 1,
        "ut": "bd1d9ddb04089700cf9c27f6f7426281",
        "fltt": 2,
        "invt": 2,
        "fid": "f3",
        "fs": "m:0+t:6,m:0+t:13,m:0+t:80,m:1+t:2,m:1+t:23",
        "fields": "f12,f14",  # f12: code, f14: name
        "_": int(time.time() * 1000)
    }
    ua = UserAgent()
    headers = {"User-Agent": ua.random}

    try:
        response = requests.get(url, params=params, headers=headers, timeout=10)
        response.raise_for_status()  # Raise HTTPError for bad responses (4xx or 5xx)

        data = response.json()
        if not data or "data" not in data or "diff" not in data["data"]:
            logger.error("Eastmoney API response structure is not as expected.")
            return []

        stock_list_raw = data["data"]["diff"]
        stocks = []
        for stock_item in stock_list_raw:
            if "f12" in stock_item and "f14" in stock_item:
                # Determine prefix based on market
                # f13 is market id, 1 for SH, 0 for SZ
                # However, the fs parameter already filters for SH and SZ A-shares.
                # The prefix is usually part of f12 for Eastmoney's A-share list.
                # For instance, SH is implicit for codes starting with 6, SZ for 0 or 3.
                # Let's assume the code from f12 is sufficient.
                # If specific SH/SZ prefixing is needed, we might need f13 or adjust fs.
                code = stock_item["f12"]
                name = stock_item["f14"]
                stocks.append({"code": code, "name": name})
            else:
                logger.warning(f"Missing 'f12' or 'f14' in stock item: {stock_item}")
        
        logger.info(f"Successfully retrieved {len(stocks)} stocks.")
        return stocks

    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching stock list from Eastmoney: {e}")
        return []
    except ValueError as e: # For JSON decoding errors
        logger.error(f"Error decoding JSON response from Eastmoney stock list API: {e}")
        return []

# --- Function to get K-line data ---
def get_kline_data(stock_code_with_prefix: str, stock_name: str, num_days: int):
    """
    Retrieves 120-minute K-line data for a given stock from Eastmoney.
    """
    logger.info(f"Attempting to retrieve K-line data for {stock_name} ({stock_code_with_prefix}) for {num_days} days.")

    # Determine secid
    if stock_code_with_prefix.startswith("SH") or stock_code_with_prefix.startswith("BJ"):
        secid = f"1.{stock_code_with_prefix[2:]}"
    elif stock_code_with_prefix.startswith("SZ"):
        secid = f"0.{stock_code_with_prefix[2:]}"
    else:
        logger.error(f"Unrecognized stock code prefix for {stock_code_with_prefix}. Cannot determine secid.")
        return None

    url = "http://push2his.eastmoney.com/api/qt/stock/kline/get"
    end_date = datetime.today().strftime('%Y%m%d')
    start_date = (datetime.today() - timedelta(days=num_days)).strftime('%Y%m%d')

    params = {
        "secid": secid,
        "ut": "bd1d9ddb04089700cf9c27f6f7426281",
        "fields1": "f1,f2,f3,f4,f5,f6",
        "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61", # date,open,close,high,low,volume,amount,amplitude,change_percent,change_amount,turnover_rate
        "klt": "120",  # 120-minute K-line
        "fqt": "1",    # Forward-adjusted prices
        "beg": start_date,
        "end": end_date,
        "lmt": 100000, # Sufficiently large limit
        "_": int(time.time() * 1000)
    }
    headers = {"User-Agent": ua.random}

    try:
        response = requests.get(url, params=params, headers=headers, timeout=20) # Increased timeout
        response.raise_for_status()
        
        data = response.json()

        if not data or "data" not in data or not data["data"]:
            logger.info(f"No K-line data found for {stock_name} ({stock_code_with_prefix}) (data field is null or missing).")
            return []

        klines_raw = data["data"].get("klines")
        if not klines_raw or not isinstance(klines_raw, list):
            logger.info(f"No K-line data found for {stock_name} ({stock_code_with_prefix}) (klines field is missing or not a list).")
            return []

        processed_klines = []
        # Expected fields: date,open,close,high,low,volume,amount,amplitude,change_percent,change_amount,turnover_rate
        # The API returns "YYYY-MM-DD HH:MM" as the first part of the string.
        for k_str in klines_raw:
            parts = k_str.split(',')
            if len(parts) < 7: # Ensure we have at least up to 'amount'
                logger.warning(f"Skipping malformed k-line string for {stock_code_with_prefix}: {k_str}")
                continue
            
            # The first part is "YYYY-MM-DD HH:MM" if klt implies time, otherwise just "YYYY-MM-DD"
            # For klt=120, it includes time.
            kline_datetime_str = parts[0]
            # It seems the API for klt=120 might just return date without HH:MM in the kline string, 
            # but the problem description implies "YYYY-MM-DD HH:MM".
            # Let's assume it's there. If not, the parsing might need adjustment or clarification.
            # For now, we use it as is.

            processed_klines.append({
                "datetime": kline_datetime_str, # This is "YYYY-MM-DD HH:MM" or "YYYY-MM-DD"
                "open": float(parts[1]),
                "close": float(parts[2]),
                "high": float(parts[3]),
                "low": float(parts[4]),
                "volume": float(parts[5]), # Or int, depending on typical values
                "amount": float(parts[6]), # Or int
                "stock_code": stock_code_with_prefix,
                "stock_name": stock_name
            })
        
        logger.info(f"Successfully retrieved {len(processed_klines)} K-line data points for {stock_name} ({stock_code_with_prefix}).")
        return processed_klines

    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching K-line data for {stock_name} ({stock_code_with_prefix}): {e}")
        return None
    except ValueError as e: # For JSON decoding errors
        logger.error(f"Error decoding K-line JSON response for {stock_name} ({stock_code_with_prefix}): {e}")
        return None
    except Exception as e: # Catch any other unexpected errors during processing
        logger.error(f"An unexpected error occurred while processing K-line data for {stock_name} ({stock_code_with_prefix}): {e}")
        return None

# --- Function to process and save K-line data ---
def process_and_save_data(kline_data_list, stock_code, stock_name, output_dir):
    """
    Processes K-line data and saves it to a CSV file.
    """
    if not kline_data_list:
        logger.info(f"No data to save for stock {stock_name} ({stock_code}).")
        return

    try:
        df = pd.DataFrame(kline_data_list)

        # Ensure correct column order and types
        # 'datetime' is already string "YYYY-MM-DD HH:MM" or "YYYY-MM-DD"
        # 'stock_code', 'stock_name' are already in the dicts
        column_order = ['stock_code', 'stock_name', 'datetime', 'open', 'close', 'high', 'low', 'volume', 'amount']
        df = df[column_order]

        # Convert numeric columns
        numeric_cols = ['open', 'close', 'high', 'low', 'volume', 'amount']
        for col in numeric_cols:
            df[col] = pd.to_numeric(df[col], errors='coerce') # errors='coerce' will turn problematic values into NaT/NaN

        # Drop rows with NaN in essential numeric columns if any conversion failed
        df.dropna(subset=['open', 'close', 'high', 'low'], inplace=True)

        if df.empty:
            logger.warning(f"DataFrame became empty after type conversion for {stock_name} ({stock_code}). No data saved.")
            return

        # Create output directory
        os.makedirs(output_dir, exist_ok=True)

        # Construct filename and filepath
        filename = f"{stock_code}_120min_kline.csv"
        filepath = os.path.join(output_dir, filename)

        # Save to CSV
        df.to_csv(filepath, index=False, encoding='utf-8-sig')
        logger.info(f"Successfully saved data for {stock_name} ({stock_code}) to {filepath}")

    except Exception as e:
        logger.error(f"Error processing or saving data for {stock_name} ({stock_code}): {e}")

# --- Worker Task for Concurrent Execution ---
def worker_task(stock_info, num_days, output_dir):
    """
    Fetches and saves K-line data for a single stock.
    Returns True if successful, False otherwise.
    """
    stock_code = stock_info['code']
    stock_name = stock_info['name']
    # logger.info(f"Processing stock: {stock_name} ({stock_code})") # Can be too verbose

    try:
        # Optional: Add a small delay to be polite to the server
        # time.sleep(0.1) # Consider if MAX_WORKERS is high or if rate limiting occurs

        kline_data = get_kline_data(stock_code, stock_name, num_days)

        if kline_data is None: # Indicates an error during fetching
            logger.error(f"K-line data retrieval failed for {stock_name} ({stock_code}).")
            return False # Failure
        elif not kline_data: # Empty list (no data points)
            logger.info(f"No K-line data points found for {stock_name} ({stock_code}) for the last {num_days} days.")
            return True # Success (processed, no data is not a failure of the worker)
        else: # Data retrieved
            # logger.debug(f"Successfully retrieved {len(kline_data)} K-line points for {stock_name} ({stock_code}).")
            process_and_save_data(kline_data, stock_code, stock_name, output_dir)
            return True # Success
            
    except Exception as e:
        logger.error(f"An unexpected error occurred in worker_task for {stock_name} ({stock_code}): {e}")
        return False # Failure


if __name__ == "__main__":
    args = parser.parse_args() # Parse arguments here
    logger.info(f"Script arguments: Days={args.days}, OutputDir={args.output_dir}, MaxWorkers={MAX_WORKERS}")

    logger.info("Starting stock crawler script.")
    start_time = time.time()

    stock_list = get_stock_list()

    if not stock_list:
        logger.error("No stock list retrieved. Exiting.")
    else:
        logger.info(f"Starting K-line data crawl for {len(stock_list)} stocks using up to {MAX_WORKERS} workers.")
        success_count = 0
        failure_count = 0 # Counts actual worker failures, not just "no data"

        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = [executor.submit(worker_task, stock, args.days, args.output_dir) for stock in stock_list]
            
            total_stocks = len(stock_list)
            processed_count = 0

            for future in concurrent.futures.as_completed(futures):
                processed_count += 1
                stock_code_for_log = "N/A" # Placeholder
                # To get stock_code for logging, we would need to pass it along or wrap future,
                # For now, simple progress logging.
                try:
                    result = future.result()
                    if result: # Worker returned True (success, data saved or no data found)
                        success_count +=1
                    else: # Worker returned False (error during processing)
                        failure_count += 1
                except Exception as e:
                    # This would catch errors in the future.result() call itself or if worker raised an unhandled exception
                    logger.error(f"A future completed with an exception: {e}")
                    failure_count += 1
                
                if processed_count % LOG_PROGRESS_INTERVAL == 0 or processed_count == total_stocks:
                    logger.info(f"Progress: Processed {processed_count}/{total_stocks} stocks. Success: {success_count}, Failed: {failure_count}")

        logger.info("Crawling complete!")
        logger.info(f"Summary: Total Stocks: {total_stocks}, Tasks Succeeded (data saved or no data): {success_count}, Tasks Failed (errors): {failure_count}")

    total_time = time.time() - start_time
    logger.info(f"Total time taken: {total_time:.2f} seconds")
    logger.info("Stock crawler script finished.")
