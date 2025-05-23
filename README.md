# Eastmoney A-Share K-Line Data Crawler

## Description
This script fetches 120-minute K-line (candlestick) data for all A-share stocks listed on the Shanghai (SH) and Shenzhen (SZ) exchanges from Eastmoney's public API. The retrieved data is then saved into CSV files, with each stock's data in a separate file.

## Features
*   Fetches data for all A-share stocks (SH and SZ markets, including Beijing Stock Exchange - BJ).
*   Retrieves 120-minute interval K-line data.
*   Configurable number of recent days for which to fetch historical data.
*   Saves data in CSV format, with one file generated per stock.
*   Utilizes concurrency (`ThreadPoolExecutor`) for faster data downloads.
*   Comprehensive logging of operations and errors to both `crawler.log` and the console.

## Prerequisites
*   Python 3.x

## Setup & Installation
1.  **Get the code**:
    *   Clone the repository: `git clone <repository_url>`
    *   Alternatively, download the `stock_kline_crawler.py` and `requirements.txt` files.

2.  **Install dependencies**:
    Navigate to the directory containing the script and run:
    ```bash
    pip install -r requirements.txt
    ```

## Usage
Run the script from the command line using Python.

**Basic command**:
```bash
python stock_kline_crawler.py
```
This will run the script with default settings (fetch 30 days of data and save to `./data` directory).

**Command-line arguments**:
*   `--days <number>`: Specifies the number of recent days of K-line data to fetch.
    *   Example: Fetch the last 60 days of data.
        ```bash
        python stock_kline_crawler.py --days 60
        ```
*   `--output_dir <path>`: Specifies the directory where the output CSV files will be saved. The directory will be created if it doesn't exist.
    *   Example: Save data to a directory named `./stock_data`.
        ```bash
        python stock_kline_crawler.py --output_dir ./stock_data
        ```
    *   You can combine arguments:
        ```bash
        python stock_kline_crawler.py --days 90 --output_dir ./market_data_90d
        ```

## Output
*   **CSV Files**:
    *   Format: `[STOCK_CODE]_[INTERVAL]_kline.csv` (e.g., `SH600519_120min_kline.csv`, `SZ000001_120min_kline.csv`).
    *   Location: Stored in the directory specified by `--output_dir` (defaults to `./data`).
    *   Columns in each CSV file:
        1.  `stock_code`: The stock code with prefix (e.g., SH600519).
        2.  `stock_name`: The name of the stock.
        3.  `datetime`: The date and time of the K-line data point (format: "YYYY-MM-DD HH:MM" for 120-min interval).
        4.  `open`: Opening price.
        5.  `close`: Closing price.
        6.  `high`: Highest price.
        7.  `low`: Lowest price.
        8.  `volume`: Trading volume.
        9.  `amount`: Trading amount (turnover).

*   **Log File**:
    *   `crawler.log`: Contains detailed logs of the script's operations, including successful fetches, any errors encountered, and progress updates. This file is created in the same directory where the script is run.

## Configuration (Advanced)
The script contains a couple of internal constants that can be adjusted for performance tuning if needed:
*   `MAX_WORKERS`: (Default: 10) The maximum number of concurrent threads used to download data. Increasing this may speed up downloads but can also put more load on your network and the remote server.
*   `LOG_PROGRESS_INTERVAL`: (Default: 50) How often (in terms of number of stocks processed) to log a progress update.

These constants are found near the top of the `stock_kline_crawler.py` script.

## Error Handling & Troubleshooting
*   **Network Errors**: Ensure you have a stable internet connection. Proxies or firewalls might interfere with requests.
*   **API Changes**: Eastmoney may change its API endpoints or data format. This would require updates to the script's code.
*   **Rate Limiting/Blocking**: If you make too many requests in a short period, your IP might be temporarily rate-limited or blocked by the server. If this occurs:
    *   Try reducing the `MAX_WORKERS` constant in the script.
    *   Consider introducing a small delay within the `worker_task` function (a `time.sleep(0.1)` is commented out as an example).
*   **Check Logs**: Always check the `crawler.log` file for detailed error messages. This can provide clues about what went wrong.

## Disclaimer
*   The data provided by this script is sourced from Eastmoney.com. Users should ensure they are in compliance with Eastmoney's terms of service and any applicable data usage policies.
*   This script is intended for educational and personal use only. Use it responsibly and ethically.
*   There are no guarantees regarding the accuracy, completeness, or availability of the data. Market data can be subject to errors or delays.
