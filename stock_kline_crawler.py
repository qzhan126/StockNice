import logging
import argparse
import time
from datetime import datetime, timedelta
import os
import pandas as pd
import requests
from fake_useragent import UserAgent
import concurrent.futures # Added concurrent.futures
import re # Added for secid validation

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
    logger.info("Attempting to retrieve full stock list from Eastmoney with pagination...")
    url = "http://push2.eastmoney.com/api/qt/clist/get"
    page_size = 200  # Moderate page size
    all_stocks_data_raw = [] # To accumulate raw stock items from all pages
    
    base_params = {
        "po": 1, "np": 1, "ut": "bd1d9ddb04089700cf9c27f6f7426281",
        "fltt": 2, "invt": 2, "fid": "f3",
        "fs": "m:0+t:6,m:0+t:13,m:0+t:80,m:1+t:2,m:1+t:23",
        "fields": "f12,f13,f14", # f12: code, f13: secid, f14: name
        "pz": page_size
    }
    headers = {"User-Agent": ua.random} # ua is global

    # --- Initial Request ---
    logger.info(f"Fetching first page of stock list (pn=1, pz={page_size})...")
    current_params = base_params.copy()
    current_params["pn"] = 1
    current_params["_"] = int(time.time() * 1000)

    try:
        response = requests.get(url, params=current_params, headers=headers, timeout=10)
        response.raise_for_status()
        data = response.json()

        if not data or "data" not in data:
            logger.error("Eastmoney API response structure for initial page is not as expected (missing 'data' field).")
            return []

        total_stocks_from_api = data["data"].get("total", 0)
        logger.info(f"Eastmoney API reports total of {total_stocks_from_api} stocks for the query.")

        if total_stocks_from_api == 0:
            logger.warning("API reports 0 total stocks. Returning empty list.")
            return []
            
        current_page_stocks = data["data"].get("diff", [])
        if not current_page_stocks:
            logger.warning("No stocks found on the first page ('diff' field is empty or missing), despite API reporting total > 0.")
            # Depending on API behavior, we might still continue if total_stocks_from_api > 0
            # For now, if first page is empty but total > 0, it's suspicious.
            # However, the loop condition will handle fetching subsequent pages if num_pages > 1.
        
        all_stocks_data_raw.extend(current_page_stocks)
        logger.info(f"Retrieved {len(current_page_stocks)} stocks from the first page.")

    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching initial page of stock list: {e}", exc_info=True)
        return [] # Abort if the first page fails
    except ValueError as e: # JSON decoding error
        logger.error(f"Error decoding JSON for initial page of stock list: {e}", exc_info=True)
        return []

    # --- Pagination Loop ---
    if total_stocks_from_api <= page_size:
        logger.info("All stocks fetched on the first page.")
    else:
        num_pages = (total_stocks_from_api + page_size - 1) // page_size
        logger.info(f"Total pages to fetch: {num_pages}")

        for page_num in range(2, num_pages + 1):
            logger.info(f"Fetching page {page_num}/{num_pages} from stock list API...")
            current_params["pn"] = page_num
            current_params["_"] = int(time.time() * 1000)
            
            try:
                response = requests.get(url, params=current_params, headers=headers, timeout=10)
                response.raise_for_status()
                page_data = response.json()

                if not page_data or "data" not in page_data or "diff" not in page_data["data"]:
                    logger.warning(f"Page {page_num} response structure error or missing 'diff'. Skipping this page.")
                    continue # Try next page

                current_page_stocks = page_data["data"]["diff"]
                if not current_page_stocks:
                    logger.warning(f"No stocks found on page {page_num} ('diff' field is empty).")
                    # Consider if we should break here. If total is reliable, missing data on an intermediate page is odd.
                    # For now, continue, as other pages might still have data.
                    continue 
                
                all_stocks_data_raw.extend(current_page_stocks)
                logger.info(f"Retrieved {len(current_page_stocks)} stocks from page {page_num}. Total accumulated: {len(all_stocks_data_raw)}")
                time.sleep(0.5) # Be polite to the server

            except requests.exceptions.RequestException as e:
                logger.error(f"Error fetching page {page_num} of stock list: {e}", exc_info=True)
                # Continue to the next page
            except ValueError as e: # JSON decoding error
                logger.error(f"Error decoding JSON for page {page_num} of stock list: {e}", exc_info=True)
                # Continue to the next page

    # --- Processing Paginated Results ---
    final_stock_list = []
    # Regex for basic secid validation (e.g., "0.dddddd", "1.dddddd", "8.dddddd" - though Eastmoney uses 0. and 1. for A-shares)
    # Stricter: A-shares are typically 0. (SZ) or 1. (SH/BJ) followed by 6 digits.
    secid_validation_regex = re.compile(r"^[01]\.\d{6}$") 

    logger.debug(f"Starting processing of {len(all_stocks_data_raw)} raw stock items from API.")
    for item_index, stock_item in enumerate(all_stocks_data_raw):
        # logger.debug(f"Raw stock data item {item_index + 1}: {stock_item}") # Uncomment for very detailed debug

        f12_code = stock_item.get('f12')
        f13_value = stock_item.get('f13') # This is the secid candidate or market type
        f14_name = stock_item.get('f14')

        if not f12_code or not f14_name:
            logger.warning(f"Missing f12 (code: '{f12_code}') or f14 (name: '{f14_name}') in item: {stock_item}. Skipping.")
            continue

        secid_to_validate = None

        if isinstance(f13_value, int):
            if f13_value == 0: # Typically SZ market
                # Check if f12_code is 6 digits; regex later will also check this.
                if len(f12_code) == 6 and f12_code.isdigit():
                    secid_to_validate = f"0.{f12_code}"
                    logger.info(f"f13 is integer 0 for code {f12_code}. Reconstructed secid to '{secid_to_validate}'.")
                else:
                    logger.warning(f"f13 is integer 0, but f12_code '{f12_code}' is not a 6-digit string. Skipping.")
                    continue
            elif f13_value == 1: # Typically SH or BJ market
                if len(f12_code) == 6 and f12_code.isdigit():
                    secid_to_validate = f"1.{f12_code}"
                    logger.info(f"f13 is integer 1 for code {f12_code}. Reconstructed secid to '{secid_to_validate}'.")
                else:
                    logger.warning(f"f13 is integer 1, but f12_code '{f12_code}' is not a 6-digit string. Skipping.")
                    continue
            else: # Other integer values for f13
                logger.warning(f"Unsupported integer f13 value: {f13_value} for stock code {f12_code}. Skipping.")
                continue
        elif isinstance(f13_value, str):
            secid_to_validate = f13_value
        else: # f13_value is not an int or str (e.g., None, or other type)
            logger.warning(f"Invalid or missing f13 type (value: '{f13_value}') for stock code {f12_code}. Skipping.")
            continue
        
        # Now, validate secid_to_validate
        if secid_to_validate and secid_validation_regex.match(secid_to_validate):
            final_stock_list.append({"code": f12_code, "name": f14_name, "secid": secid_to_validate})
        else:
            logger.warning(f"Constructed or provided secid '{secid_to_validate}' for stock code {f12_code} failed validation or was None. Skipping.")
            # No continue here, as it's the end of the loop iteration
            
    logger.info(f"Successfully processed {len(final_stock_list)} stock entries with valid secids (accumulated from {len(all_stocks_data_raw)} raw items).")
    if len(final_stock_list) != total_stocks_from_api and total_stocks_from_api > 0:
         logger.warning(f"Mismatch: API reported {total_stocks_from_api} stocks, but processed {len(final_stock_list)} after secid validation and reconstruction. Some data might be missing or had invalid/unsupported f13 values.")
    
    return final_stock_list

# --- Function to get K-line data ---
# Removed extraneous "return []" that was here
def get_kline_data(secid: str, stock_code: str, stock_name: str, num_days: int):
    """
    Retrieves 120-minute K-line data for a given stock from Eastmoney using its secid.
    """
    # --- secid Validation ---
    # Regex for A-share secid validation (e.g., "0.dddddd", "1.dddddd")
    # Consistent with the one in get_stock_list.
    secid_kline_regex = re.compile(r"^[01]\.\d{6}$") 
    if not isinstance(secid, str) or not secid_kline_regex.match(secid):
        logger.error(f"Invalid secid format received in get_kline_data: '{secid}' for stock {stock_code} ({stock_name}). Aborting K-line fetch for this stock.")
        return None # Or return [] if that's more consistent, but None for error is fine.
    # --- End secid Validation ---

    logger.info(f"Attempting to retrieve K-line data for {stock_name} ({stock_code}, secid: {secid}) for {num_days} days.")

    # secid is now directly passed as an argument and validated.
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
    stock_list_full = get_stock_list() # Renamed to avoid confusion with processed_stock_list
    
    if not stock_list_full:
        logger.error("No stock list retrieved from get_stock_list(). Exiting full crawl.")
    else:
        logger.info(f"Retrieved {len(stock_list_full)} total stocks from get_stock_list() for processing.")

        # --- Temporary Slicing & Specific Stock Inclusion for K-line Processing ---
        problematic_stock_code = "300584" # 海辰药业
        problematic_stock_info = next((s for s in stock_list_full if s['code'] == problematic_stock_code), None)
        
        test_slice_size = 250 
        processed_stock_list = [] # Initialize

        if len(stock_list_full) > test_slice_size:
            logger.info(f"Slicing full stock list from {len(stock_list_full)} to {test_slice_size} for K-line fetching test run.")
            processed_stock_list = stock_list_full[:test_slice_size]
        else:
            processed_stock_list = list(stock_list_full) # Use a copy
            logger.info(f"Full stock list size ({len(stock_list_full)}) is within test slice size. Processing all retrieved stocks.")

        if problematic_stock_info:
            # Check if it's already in the (potentially sliced) list
            if not any(s['code'] == problematic_stock_code for s in processed_stock_list):
                processed_stock_list.append(problematic_stock_info)
                logger.info(f"Added problematic stock {problematic_stock_code} ('{problematic_stock_info['name']}') to the current test batch. New batch size: {len(processed_stock_list)} stocks.")
            else:
                logger.info(f"Problematic stock {problematic_stock_code} ('{problematic_stock_info['name']}') already in the sliced list.")
        elif problematic_stock_code: # Only warn if a code was specified
            logger.warning(f"Problematic stock code {problematic_stock_code} not found in the full list from get_stock_list().")
        # --- End Slicing & Specific Stock Inclusion ---

        logger.info(f"Preparing to process {len(processed_stock_list)} stocks for K-line data using up to {MAX_WORKERS} workers.")
        success_count = 0
        failure_count = 0 
        
        futures = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            logger.debug("Starting task submission to ThreadPoolExecutor for processed_stock_list.")
            for stock_item in processed_stock_list: # Use processed_stock_list here
                logger.debug(f"Submitting task for {stock_item.get('code', 'N/A')} - {stock_item.get('name', 'N/A')}")
                futures.append(executor.submit(worker_task, stock_item, args.days, args.output_dir))
            logger.info(f"All {len(futures)} tasks submitted to executor from processed_stock_list.")
            
            # total_stocks should now reflect the count of the list being processed
            total_stocks_to_process = len(processed_stock_list) 
            processed_count = 0
    
            logger.debug("Waiting for tasks to complete (using as_completed).")
            for future in concurrent.futures.as_completed(futures):
                processed_count += 1
                logger.debug(f"Future completed. Processing result for task {processed_count}/{total_stocks_to_process}...")
                try:
                    result_from_future = future.result()
                    if result_from_future: 
                        success_count +=1
                    else: 
                        failure_count += 1
                    logger.debug(f"Result for a task: {result_from_future}. Current Success: {success_count}, Current Failure: {failure_count}")
                except Exception as e:
                    logger.error(f"A task resulted in an exception: {e}", exc_info=True)
                    failure_count += 1
                
                if processed_count % LOG_PROGRESS_INTERVAL == 0 or processed_count == total_stocks_to_process:
                    logger.info(f"Progress: Processed {processed_count}/{total_stocks_to_process} stocks. Success: {success_count}, Failed: {failure_count}")
    
        logger.info("K-line data crawling complete for the processed list!")
        logger.info(f"Summary for processed list: Total Stocks: {total_stocks_to_process}, Tasks Succeeded: {success_count}, Tasks Failed: {failure_count}")

    end_time = time.time()
    total_time = end_time - start_time
    logger.info(f"Total time taken for script execution (including full get_stock_list and processed K-line fetch): {total_time:.2f} seconds")
    logger.info("Stock crawler script finished.")
