import pandas as pd
import numpy as np
import yfinance as yf
import time
import schedule
import requests
import logging
from datetime import datetime, timedelta
import os
import json
import random
from typing import List, Dict, Tuple, Optional
import backoff

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("trading_bot.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Suppress excessive yfinance warnings
logging.getLogger('yfinance').setLevel(logging.ERROR)
logging.getLogger('urllib3').setLevel(logging.WARNING)

# Configuration
TELEGRAM_TOKEN = os.environ.get('TELEGRAM_TOKEN', '8017759392:AAEwM-W-y83lLXTjlPl8sC_aBmizuIrFXnU')
TELEGRAM_CHAT_ID = os.environ.get('TELEGRAM_CHAT_ID', '711856868')
WHATSAPP_NUMBER = os.environ.get('WHATSAPP_NUMBER', '+918376906697')
WHATSAPP_API_URL = os.environ.get('WHATSAPP_API_URL', 'https://api.whatsapp.com/send')

# Constants
DATA_CACHE_DIR = "data_cache"
CACHE_EXPIRY_HOURS = 2  # Cache data valid for 2 hours
MAX_RETRY_ATTEMPTS = 3
BATCH_SIZE = 15  # Reduced batch size to avoid rate limiting
MIN_BATCH_DELAY = 30  # Minimum delay between batches in seconds
MAX_BATCH_DELAY = 60  # Maximum delay between batches in seconds
MIN_REQUEST_DELAY = 3  # Minimum delay between requests in seconds
MAX_REQUEST_DELAY = 7  # Maximum delay between requests in seconds

# Create cache directory if it doesn't exist
os.makedirs(DATA_CACHE_DIR, exist_ok=True)

# Initialize a shared session for all requests
def create_session():
    session = requests.Session()
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.9',
        'Accept-Encoding': 'gzip, deflate, br',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
        'Cache-Control': 'max-age=0'
    })
    return session

SHARED_SESSION = create_session()

# Load Nifty 500 stocks with failover mechanisms
def load_nifty500_stocks() -> List[str]:
    try:
        # Try the local file first for faster loading
        if os.path.exists("nifty500_list.csv"):
            try:
                df = pd.read_csv("nifty500_list.csv")
                # Check if the file has the expected format
                if 'Symbol' in df.columns:
                    symbols = [f"{symbol}.NS" for symbol in df['Symbol'].tolist()]
                    # Check file age - if more than 7 days old, try to update
                    file_age = (datetime.now() - datetime.fromtimestamp(os.path.getmtime("nifty500_list.csv"))).days
                    if file_age <= 7:
                        logger.info(f"Loaded {len(symbols)} Nifty 500 stocks from local file")
                        return symbols
                    else:
                        logger.info("Local file is older than 7 days, attempting to update from online source")
            except Exception as e:
                logger.warning(f"Failed to load from local file: {e}")
        
        # Try online source if local file doesn't exist or is outdated
        try:
            url = "https://archives.nseindia.com/content/indices/ind_nifty500list.csv"
            df = pd.read_csv(url)
            symbols = [f"{symbol}.NS" for symbol in df['Symbol'].tolist()]
            
            # Save to local file as backup
            df.to_csv("nifty500_list.csv", index=False)
            
            logger.info(f"Loaded {len(symbols)} Nifty 500 stocks from online source")
            return symbols
        except Exception as e:
            logger.warning(f"Failed to load from online source: {e}")
        
        # If we get here, try another online source
        try:
            url = "https://www1.nseindia.com/content/indices/ind_nifty500list.csv"
            df = pd.read_csv(url)
            symbols = [f"{symbol}.NS" for symbol in df['Symbol'].tolist()]
            
            # Save to local file as backup
            df.to_csv("nifty500_list.csv", index=False)
            
            logger.info(f"Loaded {len(symbols)} Nifty 500 stocks from alternate online source")
            return symbols
        except Exception as e:
            logger.warning(f"Failed to load from alternate online source: {e}")
        
        # If all online sources fail, try loading from local backup
        if os.path.exists("valid_stocks.json"):
            with open("valid_stocks.json", "r") as f:
                symbols = json.load(f)
                logger.info(f"Loaded {len(symbols)} stocks from valid_stocks.json backup")
                return symbols
        
        raise Exception("Could not load Nifty 500 stocks from any source")
    except Exception as e:
        logger.error(f"Error loading Nifty 500 stocks: {e}")
        # Return top 50 stocks as fallback
        logger.info("Using fallback list of top stocks")
        return ["RELIANCE.NS", "TCS.NS", "HDFCBANK.NS", "INFY.NS", "ICICIBANK.NS", 
                "HINDUNILVR.NS", "ITC.NS", "KOTAKBANK.NS", "LT.NS", "SBIN.NS",
                "AXISBANK.NS", "BAJFINANCE.NS", "BHARTIARTL.NS", "ASIANPAINT.NS", "MARUTI.NS"]

# Check if cached data is valid
def is_cache_valid(symbol: str, interval: str) -> bool:
    cache_file = os.path.join(DATA_CACHE_DIR, f"{symbol}_{interval}.csv")
    if not os.path.exists(cache_file):
        return False
    
    # Check file modification time
    mod_time = os.path.getmtime(cache_file)
    mod_datetime = datetime.fromtimestamp(mod_time)
    now = datetime.now()
    
    # Valid if less than CACHE_EXPIRY_HOURS old
    return (now - mod_datetime).total_seconds() < CACHE_EXPIRY_HOURS * 3600

# Backoff decorator for rate limiting
def rate_limit_backoff(max_tries=5):
    def backoff_hdlr(details):
        logger.warning(
            f"Backing off {details['wait']:0.1f} seconds after {details['tries']} tries "
            f"calling function {details['target'].__name__} with args {details['args']} and kwargs "
            f"{details['kwargs']}"
        )
        
    def giveup_hdlr(details):
        logger.error(
            f"Giving up after {details['tries']} tries calling function {details['target'].__name__}"
        )
    
    return backoff.on_exception(
        backoff.expo,
        (requests.exceptions.RequestException, json.JSONDecodeError, ValueError),
        max_tries=max_tries,
        on_backoff=backoff_hdlr,
        on_giveup=giveup_hdlr
    )

# Get stock data with caching and retry logic
def get_stock_data(symbol: str, interval: str = '30m', period: str = '7d', retry_count: int = MAX_RETRY_ATTEMPTS) -> Optional[pd.DataFrame]:
    # Clean symbol format
    clean_symbol = symbol.strip()
    
    # Check cache first
    cache_file = os.path.join(DATA_CACHE_DIR, f"{clean_symbol}_{interval}.csv")
    
    # Use cached data if valid
    if is_cache_valid(clean_symbol, interval):
        try:
            df = pd.read_csv(cache_file, index_col=0, parse_dates=True)
            if not df.empty and len(df) >= 5:
                logger.info(f"Using cached data for {clean_symbol}")
                return df
        except Exception as e:
            logger.warning(f"Error reading cache for {clean_symbol}: {e}")
    
    # Try different methods to download data
    for attempt in range(retry_count):
        try:
            # Add random delay to avoid pattern detection
            time.sleep(random.uniform(MIN_REQUEST_DELAY, MAX_REQUEST_DELAY))
            
            logger.info(f"Downloading data for {clean_symbol} (attempt {attempt+1}/{retry_count})")
            
            # Try various symbol formats
            symbol_variations = [
                clean_symbol,
                clean_symbol.replace(".NS", ""),  # Without suffix
                clean_symbol.replace(".NS", ".BO") if ".NS" in clean_symbol else clean_symbol,  # BSE alternative
                clean_symbol.replace(".BO", ".NS") if ".BO" in clean_symbol else clean_symbol,  # NSE alternative
            ]
            
            # Remove duplicates while preserving order
            symbol_variations = list(dict.fromkeys(symbol_variations))
            
            for sym_var in symbol_variations:
                try:
                    # Method 1: With specific parameters
                    data = download_with_retry(sym_var, interval, period)
                    if data is not None and not data.empty and len(data) > 5:
                        logger.info(f"Successfully downloaded data for {clean_symbol} using symbol {sym_var}")
                        data.to_csv(cache_file)
                        return data
                    
                    # Method 2: Try with a fresh Ticker object
                    ticker = yf.Ticker(sym_var)
                    if ticker.session is None:
                        ticker.session = SHARED_SESSION
                        
                    data = ticker.history(interval=interval, period=period)
                    if not data.empty and len(data) > 5:
                        logger.info(f"Successfully downloaded data for {clean_symbol} using Ticker object with symbol {sym_var}")
                        data.to_csv(cache_file)
                        return data
                except Exception as e:
                    logger.debug(f"Failed to download {sym_var}: {str(e)}")
                    continue
            
            logger.warning(f"All download methods failed for {clean_symbol} on attempt {attempt+1}")
            time.sleep(5 * (attempt + 1))  # Exponential backoff
            
        except Exception as e:
            logger.error(f"Error in download attempt for {clean_symbol}: {str(e)}")
            time.sleep(7 * (attempt + 1))
    
    logger.error(f"Failed to retrieve data for {clean_symbol} after {retry_count} attempts")
    return None

# Download with backoff retry for rate limiting
@rate_limit_backoff(max_tries=3)
def download_with_retry(symbol: str, interval: str, period: str) -> Optional[pd.DataFrame]:
    try:
        data = yf.download(
            symbol, 
            interval=interval, 
            period=period, 
            progress=False,
            threads=False  # Disable threading to reduce rate limiting issues
        )
        if data is None or data.empty or len(data) < 5:
            return None
        return data
    except (json.JSONDecodeError, ValueError) as e:
        logger.warning(f"JSON error downloading {symbol}: {str(e)}")
        raise  # Re-raise for backoff to catch
    except Exception as e:
        logger.error(f"Unexpected error downloading {symbol}: {str(e)}")
        return None

# Calculate SuperTrend indicator
def calculate_supertrend(df: pd.DataFrame, period: int = 10, multiplier: int = 3) -> pd.DataFrame:
    try:
        # Make a copy to avoid modifying the original
        df = df.copy()
        
        # Ensure we have the required columns
        required_cols = ['High', 'Low', 'Close']
        if not all(col in df.columns for col in required_cols):
            logger.error(f"Missing required columns for SuperTrend. Available columns: {df.columns.tolist()}")
            return df
        
        high = df['High']
        low = df['Low']
        close = df['Close']
        
        # Calculate ATR
        tr1 = high - low
        tr2 = abs(high - close.shift(1))
        tr3 = abs(low - close.shift(1))
        tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
        atr = tr.rolling(period).mean()
        
        # Calculate SuperTrend
        upper_band = (high + low) / 2 + multiplier * atr
        lower_band = (high + low) / 2 - multiplier * atr
        
        super_trend = pd.Series(0.0, index=df.index)
        direction = pd.Series(1, index=df.index)
        
        # Set initial values
        super_trend.iloc[0:period] = np.nan
        direction.iloc[0:period] = np.nan
        
        # Calculate SuperTrend values from period onwards
        for i in range(period, len(df)):
            if close.iloc[i] > upper_band.iloc[i-1]:
                direction.iloc[i] = 1
            elif close.iloc[i] < lower_band.iloc[i-1]:
                direction.iloc[i] = -1
            else:
                direction.iloc[i] = direction.iloc[i-1]
                
                if direction.iloc[i] == 1 and lower_band.iloc[i] < lower_band.iloc[i-1]:
                    lower_band.iloc[i] = lower_band.iloc[i-1]
                if direction.iloc[i] == -1 and upper_band.iloc[i] > upper_band.iloc[i-1]:
                    upper_band.iloc[i] = upper_band.iloc[i-1]
            
            if direction.iloc[i] == 1:
                super_trend.iloc[i] = lower_band.iloc[i]
            else:
                super_trend.iloc[i] = upper_band.iloc[i]
        
        df['SuperTrend'] = super_trend
        df['SuperTrend_Direction'] = direction
        
        return df
    except Exception as e:
        logger.error(f"Error calculating SuperTrend: {e}")
        # Return original dataframe if calculation fails
        df['SuperTrend'] = np.nan
        df['SuperTrend_Direction'] = np.nan
        return df

# Calculate Chandelier Exit indicator
def calculate_chandelier_exit(df: pd.DataFrame, period: int = 22, multiplier: int = 3) -> pd.DataFrame:
    try:
        # Make a copy to avoid modifying the original
        df = df.copy()
        
        # Ensure we have the required columns
        required_cols = ['High', 'Low', 'Close']
        if not all(col in df.columns for col in required_cols):
            logger.error(f"Missing required columns for Chandelier Exit. Available columns: {df.columns.tolist()}")
            return df
        
        high = df['High']
        low = df['Low']
        close = df['Close']
        
        # Calculate ATR
        tr1 = high - low
        tr2 = abs(high - close.shift(1))
        tr3 = abs(low - close.shift(1))
        tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
        atr = tr.rolling(period).mean()
        
        # Calculate highest high and lowest low for the period
        highest_high = high.rolling(window=period).max()
        lowest_low = low.rolling(window=period).min()
        
        # Calculate long exit and short exit
        long_exit = highest_high - multiplier * atr
        short_exit = lowest_low + multiplier * atr
        
        df['ChandelierLongExit'] = long_exit
        df['ChandelierShortExit'] = short_exit
        
        # Determine Chandelier Exit signals
        df['ChandelierExitLong'] = close > long_exit
        df['ChandelierExitShort'] = close < short_exit
        
        return df
    except Exception as e:
        logger.error(f"Error calculating Chandelier Exit: {e}")
        # Return original dataframe if calculation fails
        df['ChandelierLongExit'] = np.nan
        df['ChandelierShortExit'] = np.nan
        df['ChandelierExitLong'] = False
        df['ChandelierExitShort'] = False
        return df

# Generate trading signals
def generate_signals(symbol: str) -> Optional[Dict]:
    try:
        # Get stock data
        df = get_stock_data(symbol)
        if df is None or len(df) < 25:
            logger.warning(f"Insufficient data for {symbol} to generate signals")
            return None
        
        # Check for missing columns
        required_cols = ['Open', 'High', 'Low', 'Close', 'Volume']
        if not all(col in df.columns for col in required_cols):
            logger.warning(f"Missing required columns for {symbol}. Available: {df.columns.tolist()}")
            return None
        
        # Check for NaN values in critical columns
        for col in ['High', 'Low', 'Close']:
            if df[col].isna().sum() > 0:
                # Try to fill small gaps
                if df[col].isna().sum() < len(df) * 0.1:  # Less than 10% missing
                    df[col] = df[col].fillna(method='ffill')
                else:
                    logger.warning(f"Too many NaN values in {col} column for {symbol}")
                    return None
        
        # Calculate indicators
        df = calculate_supertrend(df)
        df = calculate_chandelier_exit(df)
        
        # Check if indicators were calculated successfully
        if df['SuperTrend'].isna().all() or df['ChandelierLongExit'].isna().all():
            logger.warning(f"Failed to calculate indicators for {symbol}")
            return None
        
        # Ensure we have at least 2 rows of data
        if len(df) < 2:
            logger.warning(f"Not enough data points for {symbol} to check for signals")
            return None
        
        # Check for signals in the latest candle
        latest = df.iloc[-1]
        previous = df.iloc[-2]
        
        signal = None
        reason = []
        
        # SuperTrend signal
        if not np.isnan(previous['SuperTrend_Direction']) and not np.isnan(latest['SuperTrend_Direction']):
            if previous['SuperTrend_Direction'] == -1 and latest['SuperTrend_Direction'] == 1:
                signal = "BUY"
                reason.append("SuperTrend turned bullish")
            elif previous['SuperTrend_Direction'] == 1 and latest['SuperTrend_Direction'] == -1:
                signal = "SELL"
                reason.append("SuperTrend turned bearish")
            
        # Chandelier Exit signal
        if isinstance(previous['ChandelierExitLong'], bool) and isinstance(latest['ChandelierExitLong'], bool):
            if previous['ChandelierExitLong'] == False and latest['ChandelierExitLong'] == True:
                if signal != "SELL":  # Avoid conflicting signals
                    signal = "BUY"
                    reason.append("Chandelier Exit Long signal")
        
        if isinstance(previous['ChandelierExitShort'], bool) and isinstance(latest['ChandelierExitShort'], bool):
            if previous['ChandelierExitShort'] == False and latest['ChandelierExitShort'] == True:
                if signal != "BUY":  # Avoid conflicting signals
                    signal = "SELL"
                    reason.append("Chandelier Exit Short signal")
        
        if signal:
            return {
                'symbol': symbol,
                'signal': signal,
                'price': latest['Close'],
                'time': df.index[-1].strftime('%Y-%m-%d %H:%M'),
                'reason': ', '.join(reason)
            }
        return None
    except Exception as e:
        logger.error(f"Error generating signals for {symbol}: {e}")
        return None

# Send Telegram message with retry mechanism
def send_telegram_message(message: str, max_retries: int = 3) -> bool:
    for attempt in range(max_retries):
        try:
            url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
            payload = {
                'chat_id': TELEGRAM_CHAT_ID,
                'text': message,
                'parse_mode': 'Markdown'
            }
            response = requests.post(url, data=payload, timeout=10)
            if response.status_code == 200:
                logger.info("Telegram message sent successfully")
                return True
            else:
                error_msg = f"Failed to send Telegram message: {response.status_code} - {response.text}"
                logger.error(error_msg)
                
                # Check for specific errors and handle them
                if response.status_code == 429:  # Too Many Requests
                    retry_after = int(response.headers.get('Retry-After', 30))
                    logger.info(f"Rate limited. Waiting {retry_after} seconds before retry")
                    time.sleep(retry_after)
                elif response.status_code in [400, 401, 404]:  # Bad request, unauthorized, not found
                    logger.error("Telegram API error. Check your bot token and chat ID")
                    return False  # Don't retry for these errors
                
                time.sleep(5 * (attempt + 1))  # Exponential backoff
        except requests.exceptions.RequestException as e:
            logger.error(f"Network error sending Telegram message: {e}")
            time.sleep(5 * (attempt + 1))
        except Exception as e:
            logger.error(f"Error sending Telegram message: {e}")
            time.sleep(5 * (attempt + 1))
    
    logger.error(f"Failed to send Telegram message after {max_retries} attempts")
    return False

# Send WhatsApp message (This is a placeholder - you'll need to integrate with a proper WhatsApp API)
def send_whatsapp_message(message: str) -> bool:
    try:
        # This is a placeholder for WhatsApp Business API integration
        logger.info(f"WhatsApp message would be sent to {WHATSAPP_NUMBER}: {message}")
        
        # Example code for when you have a proper API integration:
        # url = "https://your-whatsapp-api-endpoint.com/send"
        # payload = {
        #     'phone': WHATSAPP_NUMBER,
        #     'message': message
        # }
        # response = requests.post(url, json=payload, headers={'Authorization': 'your-auth-token'})
        # return response.status_code == 200
        
        return True
    except Exception as e:
        logger.error(f"Error sending WhatsApp message: {e}")
        return False

# Format signal message
def format_signal_message(signal: Dict) -> str:
    emoji = "🟢" if signal['signal'] == "BUY" else "🔴"
    return f"{emoji} *{signal['signal']} SIGNAL*\n\n" \
           f"*Stock:* {signal['symbol'].replace('.NS', '').replace('.BO', '')}\n" \
           f"*Price:* ₹{signal['price']:.2f}\n" \
           f"*Time:* {signal['time']}\n" \
           f"*Reason:* {signal['reason']}\n\n" \
           f"*Indicators:* SuperTrend & Chandelier Exit (30m)"

# Process all stocks for signals with improved batching and error handling
def process_stocks():
    logger.info("Starting stock analysis...")
    start_time = time.time()
    
    # Load stock list
    stocks = load_nifty500_stocks()
    random.shuffle(stocks)  # Randomize order to avoid detection patterns
    
    signals_found = False
    skipped_stocks = []
    processed_count = 0
    
    # Process in smaller batches to avoid rate limiting
    for i in range(0, len(stocks), BATCH_SIZE):
        batch = stocks[i:i+BATCH_SIZE]
        batch_start_time = time.time()
        logger.info(f"Processing batch {i//BATCH_SIZE + 1}/{(len(stocks)//BATCH_SIZE) + 1} ({len(batch)} stocks)")
        
        batch_signals = []
        
        for symbol in batch:
            try:
                processed_count += 1
                signal = generate_signals(symbol)
                if signal:
                    batch_signals.append(signal)
                    signals_found = True
                    logger.info(f"Signal found for {symbol}: {signal['signal']}")
                
                # Add rate limiting between requests
                time.sleep(random.uniform(MIN_REQUEST_DELAY, MAX_REQUEST_DELAY))
            except Exception as e:
                logger.error(f"Error processing {symbol}: {e}")
                skipped_stocks.append(symbol)
                time.sleep(MAX_REQUEST_DELAY)  # Longer delay after error
        
        # Send batch notifications
        for signal in batch_signals:
            message = format_signal_message(signal)
            send_telegram_message(message)
            send_whatsapp_message(message.replace('*', ''))  # Remove markdown for WhatsApp
            time.sleep(1)  # Brief delay between messages
        
        batch_duration = time.time() - batch_start_time
        logger.info(f"Batch {i//BATCH_SIZE + 1} completed in {batch_duration:.2f} seconds")
        
        # Add substantial delay between batches
        if i + BATCH_SIZE < len(stocks):  # If there are more batches to process
            delay = random.uniform(MIN_BATCH_DELAY, MAX_BATCH_DELAY)
            logger.info(f"Waiting {delay:.2f} seconds before next batch")
            time.sleep(delay)
    
    total_duration = time.time() - start_time
    logger.info(f"Stock analysis completed in {total_duration:.2f} seconds")
    logger.info(f"Processed {processed_count}/{len(stocks)} stocks, skipped {len(skipped_stocks)}")
    
    if skipped_stocks:
        logger.warning(f"Skipped stocks: {', '.join(skipped_stocks[:10])}" + 
                      (f" and {len(skipped_stocks) - 10} more" if len(skipped_stocks) > 10 else ""))
    
    if not signals_found:
        logger.info("No signals found in this scan")
        # Optionally send a "no signals" notification
        # send_telegram_message("Scan complete - No trading signals found at this time.")

# Test a single stock for debugging
def test_single_stock(symbol: str):
    logger.info(f"Testing signal generation for {symbol}")
    signal = generate_signals(symbol)
    if signal:
        message = format_signal_message(signal)
        logger.info(f"Signal found: {message}")
        return True
    else:
        logger.info(f"No signal found for {symbol}")
        return False

# Validate data availability for stocks with progress tracking
def validate_stock_list(test_size: int = None):
    logger.info("Validating data availability for stock list...")
    stocks = load_nifty500_stocks()
    
    # If test_size is specified, use a smaller sample
    if test_size and 0 < test_size < len(stocks):
        stocks = random.sample(stocks, test_size)
        logger.info(f"Testing a sample of {test_size} stocks")
    
    valid_stocks = []
    invalid_stocks = []
    
    total = len(stocks)
    success_count = 0
    
    for i, symbol in enumerate(stocks):
        try:
            df = get_stock_data(symbol, retry_count=1)
            if df is not None and not df.empty:
                valid_stocks.append(symbol)
                success_count += 1
                logger.info(f"[{i+1}/{total}] ✅ {symbol}: Data available ({success_count}/{i+1} success rate)")
            else:
                invalid_stocks.append(symbol)
                logger.warning(f"[{i+1}/{total}] ❌ {symbol}: No data available")
            
            # Less aggressive rate limiting for validation
            time.sleep(random.uniform(1, 3))
        except Exception as e:
            invalid_stocks.append(symbol)
            logger.error(f"[{i+1}/{total}] ⚠️ {symbol}: Error - {e}")
            time.sleep(3)
    
    # Save valid stocks list
    with open("valid_stocks.json", "w") as f:
        json.dump(valid_stocks, f)
    
    # Save invalid stocks list for reference
    with open("invalid_stocks.json", "w") as f:
        json.dump(invalid_stocks, f)
    
    success_rate = (success_count / total) * 100 if total > 0 else 0
    logger.info(f"Validation complete. Found {len(valid_stocks)}/{total} valid stocks ({success_rate:.1f}% success rate).")
    return valid_stocks

# Cleanup expired cache files
def cleanup_cache():
    logger.info("Cleaning up expired cache files...")
    now = datetime.now()
    count = 0
    
    for filename in os.listdir(DATA_CACHE_DIR):
        filepath = os.path.join(DATA_CACHE_DIR, filename)
        if os.path.isfile(filepath):
            mod_time = os.path.getmtime(filepath)
            mod_datetime = datetime.fromtimestamp(mod_time)
            
            # Remove if older than CACHE_EXPIRY_HOURS
            if (now - mod_datetime).total_seconds() > CACHE_EXPIRY_HOURS * 3600:
                try:
                    os.remove(filepath)
                    count += 1
                except Exception as e:
                    logger.error(f"Error removing cache file {filepath}: {e}")
    
    logger.info(f"Removed {count} expired cache files")

# Rotate log files to prevent them from growing too large
def rotate_logs():
    try:
        log_file = "trading_bot.log"
        backup_log = f"trading_bot_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
        
        # Rotate log files to prevent them from growing too large
def rotate_logs():
    try:
        log_file = "trading_bot.log"
        backup_log = f"trading_bot_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
        
        # Check if log file exists and is larger than 10MB
        if os.path.exists(log_file) and os.path.getsize(log_file) > 10 * 1024 * 1024:
            # Rename current log to backup
            os.rename(log_file, backup_log)
            
            # Configure logging again
            for handler in logger.handlers[:]:
                if isinstance(handler, logging.FileHandler):
                    handler.close()
                    logger.removeHandler(handler)
            
            file_handler = logging.FileHandler(log_file)
            file_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
            logger.addHandler(file_handler)
            
            logger.info(f"Log file rotated. Previous log saved as {backup_log}")
            
            # Keep only the 5 most recent log backups
            log_backups = [f for f in os.listdir('.') if f.startswith('trading_bot_') and f.endswith('.log')]
            if len(log_backups) > 5:
                log_backups.sort()
                for old_log in log_backups[:-5]:
                    try:
                        os.remove(old_log)
                        logger.info(f"Removed old log file: {old_log}")
                    except Exception as e:
                        logger.error(f"Failed to remove old log file {old_log}: {e}")
    except Exception as e:
        logger.error(f"Error rotating log files: {e}")

# Check for system health
def check_system_health():
    try:
        logger.info("Performing system health check...")
        issues = []
        
        # Check disk space
        disk_usage = os.statvfs('/')
        free_space_mb = (disk_usage.f_bavail * disk_usage.f_frsize) / (1024 * 1024)
        if free_space_mb < 100:  # Less than 100MB free
            issues.append(f"Low disk space: {free_space_mb:.2f}MB free")
        
        # Check cache directory
        cache_file_count = len(os.listdir(DATA_CACHE_DIR))
        if cache_file_count > 1000:
            issues.append(f"Cache directory has too many files: {cache_file_count}")
        
        # Check log file size
        if os.path.exists("trading_bot.log"):
            log_size_mb = os.path.getsize("trading_bot.log") / (1024 * 1024)
            if log_size_mb > 50:  # Over 50MB
                issues.append(f"Log file is large: {log_size_mb:.2f}MB")
        
        # Report issues if any
        if issues:
            logger.warning("System health issues detected:")
            for issue in issues:
                logger.warning(f"- {issue}")
            
            # Send notification about system health issues
            message = f"⚠️ *System Health Alert*\n\n" + "\n".join([f"• {issue}" for issue in issues])
            send_telegram_message(message)
        else:
            logger.info("System health check passed - all systems normal")
        
        return len(issues) == 0  # Return True if no issues
    except Exception as e:
        logger.error(f"Error in system health check: {e}")
        return False

# Main function to schedule jobs with improved error handling
def main():
    try:
        logger.info("=" * 50)
        logger.info("Starting Nifty 500 Stock Trading Signal Bot")
        logger.info(f"Version: 2.0.0 - {datetime.now().strftime('%Y-%m-%d')}")
        logger.info("=" * 50)
        
        # Rotate logs on startup
        rotate_logs()
        
        # Clean up any expired cache files
        cleanup_cache()
        
        # Check system health
        check_system_health()
        
        # Initial validation of stock list (with a smaller sample for quicker startup)
        valid_stocks = validate_stock_list(test_size=20)
        
        # Test with a known good stock for debugging
        logger.info("Running test on a known stock...")
        test_single_stock("RELIANCE.NS")
        
        # Run immediately once at startup
        logger.info("Running initial stock scan...")
        process_stocks()
        
        # Schedule routine tasks
        logger.info("Setting up scheduled tasks...")
        
        # Schedule cache cleanup every 4 hours
        schedule.every(4).hours.do(cleanup_cache)
        
        # Schedule log rotation daily
        schedule.every().day.at("00:01").do(rotate_logs)
        
        # Schedule system health check daily
        schedule.every().day.at("06:00").do(check_system_health)
        
        # Schedule to run every 30 minutes during market hours
        # Adjust timings according to your server's timezone
        for hour in range(9, 16):  # 9 AM to 3 PM
            for minute in [15, 45]:  # Run at xx:15 and xx:45
                if hour == 15 and minute == 45:
                    continue  # Skip 3:45 PM as market closes at 3:30 PM
                
                schedule_time = f"{hour:02d}:{minute:02d}"
                schedule.every().monday.at(schedule_time).do(process_stocks)
                schedule.every().tuesday.at(schedule_time).do(process_stocks)
                schedule.every().wednesday.at(schedule_time).do(process_stocks)
                schedule.every().thursday.at(schedule_time).do(process_stocks)
                schedule.every().friday.at(schedule_time).do(process_stocks)
        
        # Schedule full stock list validation weekly
        schedule.every().sunday.at("22:00").do(validate_stock_list)
        
        logger.info("Bot scheduled successfully")
        logger.info(schedule.get_jobs())
              
        # Keep the script running with improved error handling
        consecutive_failures = 0
        while True:
            try:
                schedule.run_pending()
                time.sleep(60)  # Check every minute
                consecutive_failures = 0  # Reset counter on success
            except KeyboardInterrupt:
                logger.info("Bot shutdown requested. Exiting gracefully...")
                break
            except Exception as e:
                consecutive_failures += 1
                logger.error(f"Error in main loop: {e}")
                
                if consecutive_failures >= 5:
                    logger.critical(f"Too many consecutive failures ({consecutive_failures}). Sending alert.")
                    send_telegram_message(f"⚠️ *Critical Alert*\n\nTrading bot encountered {consecutive_failures} consecutive errors. Please check the logs.")
                    
                    if consecutive_failures >= 10:
                        logger.critical("Fatal error threshold reached. Restarting bot...")
                        # Optional: implement a restart mechanism here
                        break
                        
                # Wait before retrying (with increasing delay)
                time.sleep(min(60 * consecutive_failures, 300))  # Max 5 minutes
                
    except Exception as e:
        logger.critical(f"Fatal error in main function: {e}")
        send_telegram_message(f"🚨 *FATAL ERROR*\n\nThe trading bot has crashed: {str(e)}\n\nPlease restart the service.")

if __name__ == "__main__":
    try:
        # Install required packages if missing (useful for automatic deployment)
        missing_packages = []
        for package in ["backoff"]:
            try:
                __import__(package)
            except ImportError:
                missing_packages.append(package)
        
        if missing_packages:
            logger.info(f"Installing missing packages: {', '.join(missing_packages)}")
            import subprocess
            subprocess.check_call([sys.executable, "-m", "pip", "install", "--user"] + missing_packages)
            logger.info("Installation complete. Restarting...")
            os.execv(sys.executable, [sys.executable] + sys.argv)
        
        # Start the main function
        main()
    except Exception as e:
        logger.critical(f"Unable to start bot: {e}")
        # Try to send a notification even in case of startup failure
        try:
            requests.post(
                f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
                data={
                    'chat_id': TELEGRAM_CHAT_ID,
                    'text': f"🚨 *CRITICAL STARTUP FAILURE*\n\nThe trading bot failed to start: {str(e)}"
                }
            )
        except:
            pass  # If this fails, we can't do much more
