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
# For this task, setting to DEBUG to see all new log messages.
# Can be set back to INFO for less verbosity.
logger.setLevel(logging.DEBUG) 

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
        "fs": "m:0+t:6,m:0+t:13,m:0+t:80,m:1+t:2,m:1+t:23", # Should cover SH, SZ, BJ A-shares
        "fields": "f12,f13,f14",  # f12: code, f13: secid (market.code), f14: name
        "_": int(time.time() * 1000)
    }
    # ua is global now
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
            if "f12" in stock_item and "f13" in stock_item and "f14" in stock_item:
                code = stock_item["f12"]    # e.g., "600519"
                secid = stock_item["f13"]   # e.g., "1.600519" or "0.000001"
                name = stock_item["f14"]    # e.g., "贵州茅台"
                
                # The 'code' field in worker_task expects the stock code with prefix like 'SH600519'
                # get_kline_data uses this to construct its own secid.
                # Let's adjust what 'code' we store in the stock_list.
                # Or, we change worker_task to use secid directly.
                # For now, let's keep 'code' as f12, and add 'secid'.
                # The problem description implies worker_task will be updated later.
                # The current subtask is only about get_stock_list and its logging.
                
                stocks.append({"code": code, "name": name, "secid": secid})
            else:
                logger.warning(f"Missing 'f12', 'f13', or 'f14' in stock item: {stock_item}")
        
        logger.info(f"Successfully retrieved {len(stocks)} stocks with code, name, and secid.")
        return stocks

    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching stock list from Eastmoney: {e}")
        return []
    except ValueError as e: # For JSON decoding errors
        logger.error(f"Error decoding JSON response from Eastmoney stock list API: {e}")
        return []

# --- Function to get K-line data ---
def get_kline_data(secid: str, stock_code: str, stock_name: str, num_days: int):
    """
    Retrieves 120-minute K-line data for a given stock from Eastmoney using its secid.
    """
    logger.info(f"Attempting to retrieve K-line data for {stock_name} ({stock_code}, secid: {secid}) for {num_days} days.")

    # secid is now directly passed as an argument. No need to determine it.
    # Example secid: "1.600519" for SH, "0.000001" for SZ, "1.830777" for BJ.

    url = "http://push2his.eastmoney.com/api/qt/stock/kline/get"
    end_date = datetime.today().strftime('%Y%m%d')
    start_date = (datetime.today() - timedelta(days=num_days)).strftime('%Y%m%d')

    params = {
        "secid": secid, 
        "ut": "bd1d9ddb04089700cf9c27f6f7426281",
        "fields1": "f1,f2,f3,f4,f5,f6",
        "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61",
        "klt": "120",
        "fqt": "1",
        "beg": start_date,
        "end": end_date,
        "lmt": 100000,
        "_": int(time.time() * 1000)
    }
    
    # Using requests.Session
    with requests.Session() as session:
        session.headers.update({"User-Agent": ua.random}) # ua is global
        try:
            response = session.get(url, params=params, timeout=20) # Increased timeout
            response.raise_for_status() # Raises HTTPError for 4xx/5xx responses
            
            data = response.json()

            if not data or "data" not in data or not data["data"]:
                logger.info(f"No K-line data found for {stock_name} ({stock_code}, secid: {secid}) (data field is null or missing).")
                return [] # Consistent with existing logic for no data

            klines_raw = data["data"].get("klines")
            if not klines_raw or not isinstance(klines_raw, list):
                logger.info(f"No K-line data found for {stock_name} ({stock_code}, secid: {secid}) (klines field is missing or not a list).")
                return [] # Consistent

            processed_klines = []
            for k_str in klines_raw:
                parts = k_str.split(',')
                if len(parts) < 7:
                    logger.warning(f"Skipping malformed k-line string for {stock_code} (secid: {secid}): {k_str}")
                    continue
                
                kline_datetime_str = parts[0]
                processed_klines.append({
                    "datetime": kline_datetime_str,
                    "open": float(parts[1]),
                    "close": float(parts[2]),
                    "high": float(parts[3]),
                    "low": float(parts[4]),
                    "volume": float(parts[5]),
                    "amount": float(parts[6]),
                    "stock_code": stock_code, 
                    "stock_name": stock_name
                })
            
            logger.info(f"Successfully retrieved {len(processed_klines)} K-line data points for {stock_name} ({stock_code}, secid: {secid}).")
            return processed_klines

        except requests.exceptions.HTTPError as e:
            # Specific logging for HTTP errors (e.g., 403 Forbidden, 404 Not Found, 500 Server Error)
            logger.error(f"HTTP error for {stock_name} ({stock_code}, secid: {secid}): {e.response.status_code} - {e.response.reason}. URL: {e.request.url}")
            return None # Return None for error cases
        except requests.exceptions.RequestException as e:
            # Catches other request-related errors like ConnectionError, Timeout, TooManyRedirects
            logger.error(f"Request exception for {stock_name} ({stock_code}, secid: {secid}): {e}", exc_info=True)
            return None
        except ValueError as e: # Specifically for JSON decoding errors
            logger.error(f"Error decoding K-line JSON response for {stock_name} ({stock_code}, secid: {secid}): {e}", exc_info=True)
            return None
        except Exception as e: # Catch any other unexpected errors during processing
            logger.error(f"An unexpected error occurred while processing K-line data for {stock_name} ({stock_code}, secid: {secid}): {e}", exc_info=True)
            return None
        # Session is automatically closed when exiting the 'with' block

# --- Function to process and save K-line data ---
        return None

# --- Function to process and save K-line data ---
def process_and_save_data(kline_data_list, original_stock_code, stock_name, secid, output_dir):
    """
    Processes K-line data and saves it to a CSV file.
    Filename prefix (SH, SZ, BJ) is determined from secid.
    The 'stock_code' column in CSV will be the original_stock_code (e.g., "600519").
    """
    if not kline_data_list:
        logger.info(f"No data to save for stock {stock_name} ({original_stock_code}, secid: {secid}).")
        return

    try:
        df = pd.DataFrame(kline_data_list) # kline_data_list already contains original_stock_code

        # Ensure correct column order and types
        # 'stock_code' in df should be the original_stock_code from get_kline_data
        column_order = ['stock_code', 'stock_name', 'datetime', 'open', 'close', 'high', 'low', 'volume', 'amount']
        df = df[column_order]

        # Convert numeric columns
        numeric_cols = ['open', 'close', 'high', 'low', 'volume', 'amount']
        for col in numeric_cols:
            df[col] = pd.to_numeric(df[col], errors='coerce')

        df.dropna(subset=['open', 'close', 'high', 'low'], inplace=True)

        if df.empty:
            logger.warning(f"DataFrame became empty after type conversion for {stock_name} ({original_stock_code}). No data saved.")
            return

        # Create output directory
        os.makedirs(output_dir, exist_ok=True)

        # Construct filename
        filename_prefix = ""
        # secid format: <market_id>.<stock_digits_part> e.g., "0.301075", "1.600519"
        # BJ secid starts with 1. but stock codes start with 4, 8, or 9.
        if secid.startswith("0."):
            filename_prefix = "SZ"
        elif secid.startswith("1."):
            # Differentiate SH from BJ based on original_stock_code starting digits
            if original_stock_code.startswith('4') or original_stock_code.startswith('8') or original_stock_code.startswith('9'):
                filename_prefix = "BJ"
            else:
                filename_prefix = "SH"
        # The problem statement mentioned "secid starts with '8' or '4' or '9'" for BJ.
        # This implies secid itself, not original_stock_code.
        # However, Eastmoney secid for BJ is like "1.8xxxxx" or "1.4xxxxx".
        # The current logic using original_stock_code for BJ differentiation when secid starts with "1." is more robust.
        # Let's stick to the current logic as it correctly identifies BJ stocks based on their code patterns when secid starts with "1."
        else: # Fallback for unrecognized secid format for prefixing
            logger.warning(f"Unhandled secid format for filename prefixing: {secid} for stock {original_stock_code}. Using no prefix.")
            filename_prefix = "" # Or "XX" as a placeholder

        filename = f"{filename_prefix}{original_stock_code}_120min_kline.csv"
        filepath = os.path.join(output_dir, filename)

        # Save to CSV
        df.to_csv(filepath, index=False, encoding='utf-8-sig')
        logger.info(f"Successfully saved data for {stock_name} ({filename_prefix}{original_stock_code}) to {filepath}")

    except Exception as e:
        logger.error(f"Error processing or saving data for {stock_name} ({original_stock_code}): {e}")

# --- Worker Task for Concurrent Execution ---
def worker_task(stock_info, num_days, output_dir):
    """
    Fetches and saves K-line data for a single stock.
    Returns True if successful, False otherwise.
    """
    original_stock_code = stock_info['code'] # e.g., "600519"
    stock_name = stock_info['name']
    secid = stock_info['secid'] # e.g., "1.600519"

    # logger.info(f"Worker processing: {stock_name} ({original_stock_code}, secid: {secid})")

    try:
        # Call get_kline_data with secid, original_stock_code, and stock_name
        kline_data = get_kline_data(secid, original_stock_code, stock_name, num_days)

        if kline_data is None: 
            logger.error(f"K-line data retrieval failed for {stock_name} ({original_stock_code}, secid: {secid}).")
            return False 
        elif not kline_data: 
            logger.info(f"No K-line data points found for {stock_name} ({original_stock_code}, secid: {secid}) for the last {num_days} days.")
            return True 
        else: 
            # Call process_and_save_data with all necessary info
            process_and_save_data(kline_data, original_stock_code, stock_name, secid, output_dir)
            return True 
            
    except Exception as e:
        logger.error(f"An unexpected error occurred in worker_task for {stock_name} ({original_stock_code}, secid: {secid}): {e}")
        return False # Failure


if __name__ == "__main__":
    args = parser.parse_args() # Parse arguments here
    logger.info(f"Script arguments: Days={args.days}, OutputDir={args.output_dir}, MaxWorkers={MAX_WORKERS}")

    logger.info("Starting stock crawler script.")
    start_time = time.time()

    # --- Specific Stock Testing (Commented out for full crawl testing) ---
    # logger.info("--- Starting Specific Stock Testing ---")
    # test_stocks_info = [
    #     {'code': '301075', 'name': '多瑞医药', 'secid': '0.301075'}, # ChiNext
    #     {'code': '688001', 'name': '华兴源创', 'secid': '1.688001'}, # STAR Market
    #     {'code': '600519', 'name': '贵州茅台', 'secid': '1.600519'}, # Shanghai Main
    #     {'code': '000001', 'name': '平安银行', 'secid': '0.000001'}, # Shenzhen Main
    #     {'code': '830777', 'name': '中纺标', 'secid': '1.830777'}    # Beijing SE
    # ]
    #
    # for stock_info_test in test_stocks_info:
    #     logger.info(f"Testing specific stock: {stock_info_test['name']} ({stock_info_test['code']}, secid: {stock_info_test['secid']})")
    #     try:
    #         kline_data_test = get_kline_data(
    #             stock_info_test['secid'], 
    #             stock_info_test['code'], 
    #             stock_info_test['name'], 
    #             args.days
    #         )
    #         if kline_data_test is None:
    #             logger.error(f"Test failed for {stock_info_test['name']}: K-line data retrieval returned None.")
    #         elif not kline_data_test:
    #             logger.info(f"Test completed for {stock_info_test['name']}: No K-line data points found.")
    #         else:
    #             logger.info(f"Test for {stock_info_test['name']}: Retrieved {len(kline_data_test)} data points. Processing and saving...")
    #             process_and_save_data(
    #                 kline_data_test, 
    #                 stock_info_test['code'], 
    #                 stock_info_test['name'], 
    #                 stock_info_test['secid'], 
    #                 args.output_dir
    #             )
    #             logger.info(f"Test successful for {stock_info_test['name']}: Data processed and saved.")
    #     except Exception as e:
    #         logger.error(f"Test failed for {stock_info_test['name']} with exception: {e}", exc_info=True)
    # 
    # logger.info("--- Specific Stock Testing Complete ---")

    # --- Full Crawl Logic (Re-enabled) ---
    stock_list = get_stock_list() 
    
    if not stock_list:
        logger.error("No stock list retrieved. Exiting full crawl.")
    else:
        # Slicing logic and associated logs removed.
        # The following log will now reflect the full count from get_stock_list().
        logger.info(f"Preparing to process {len(stock_list)} stocks using up to {MAX_WORKERS} workers.")
        success_count = 0
        failure_count = 0 
        
        futures = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            logger.debug("Starting task submission to ThreadPoolExecutor.")
            for stock_item in stock_list:
                logger.debug(f"Submitting task for {stock_item.get('code', 'N/A')} - {stock_item.get('name', 'N/A')}")
                futures.append(executor.submit(worker_task, stock_item, args.days, args.output_dir))
            logger.info(f"All {len(futures)} tasks submitted to executor.")
            
            total_stocks = len(stock_list) # Same as len(futures)
            processed_count = 0
    
            logger.debug("Waiting for tasks to complete (using as_completed).")
            for future in concurrent.futures.as_completed(futures):
                processed_count += 1
                logger.debug(f"Future completed. Processing result for task {processed_count}/{total_stocks}...")
                try:
                    result_from_future = future.result()
                    if result_from_future: 
                        success_count +=1
                    else: 
                        failure_count += 1
                    logger.debug(f"Result for a task: {result_from_future}. Current Success: {success_count}, Current Failure: {failure_count}")
                except Exception as e:
                    # Log which stock this was for, if possible.
                    # This requires mapping futures to inputs, which is more involved.
                    # For now, just enhance the existing log.
                    logger.error(f"A task resulted in an exception: {e}", exc_info=True)
                    failure_count += 1
                
                if processed_count % LOG_PROGRESS_INTERVAL == 0 or processed_count == total_stocks:
                    logger.info(f"Progress: Processed {processed_count}/{total_stocks} stocks. Success: {success_count}, Failed: {failure_count}")
    
        logger.info("Crawling complete!")
        logger.info(f"Summary: Total Stocks: {total_stocks}, Tasks Succeeded (data saved or no data): {success_count}, Tasks Failed (errors): {failure_count}")

    end_time = time.time()
    total_time = end_time - start_time
    logger.info(f"Total time taken for script execution: {total_time:.2f} seconds")
    logger.info("Stock crawler script finished.")
