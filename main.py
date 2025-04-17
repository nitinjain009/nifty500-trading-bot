# main.py
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

# Configuration
TELEGRAM_TOKEN = os.environ.get('TELEGRAM_TOKEN', '8017759392:AAEwM-W-y83lLXTjlPl8sC_aBmizuIrFXnU')
TELEGRAM_CHAT_ID = os.environ.get('TELEGRAM_CHAT_ID', '711856868')
WHATSAPP_NUMBER = os.environ.get('WHATSAPP_NUMBER', '+918376906697')
WHATSAPP_API_URL = os.environ.get('WHATSAPP_API_URL', 'https://api.whatsapp.com/send')  # Example URL, you'll need a proper WhatsApp Business API

# Constants
DATA_CACHE_DIR = "data_cache"
CACHE_EXPIRY_HOURS = 2  # Cache data valid for 2 hours

# Create cache directory if it doesn't exist
os.makedirs(DATA_CACHE_DIR, exist_ok=True)

# Load Nifty 500 stocks with failover mechanisms
def load_nifty500_stocks() -> List[str]:
    try:
        # Try online source first
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
            
        # Try local file if online source fails
        if os.path.exists("nifty500_list.csv"):
            df = pd.read_csv("nifty500_list.csv")
            symbols = [f"{symbol}.NS" for symbol in df['Symbol'].tolist()]
            logger.info(f"Loaded {len(symbols)} Nifty 500 stocks from local file")
            return symbols
            
        raise Exception("Could not load Nifty 500 stocks")
    except Exception as e:
        logger.error(f"Error loading Nifty 500 stocks: {e}")
        # Return top 50 stocks as fallback
        logger.info("Using fallback list of top stocks")
        return ["RELIANCE.NS", "TCS.NS", "HDFCBANK.NS", "INFY.NS", "ICICIBANK.NS", 
                "HINDUNILVR.NS", "ITC.NS", "KOTAKBANK.NS", "LT.NS", "SBIN.NS",
                "AXISBANK.NS", "BAJFINANCE.NS", "BHARTIARTL.NS", "ASIANPAINT.NS", "MARUTI.NS"]

# Calculate SuperTrend indicator
def calculate_supertrend(df: pd.DataFrame, period: int = 10, multiplier: int = 3) -> pd.DataFrame:
    high = df['High']
    low = df['Low']
    close = df['Close']
    
    # Calculate ATR
    tr1 = pd.DataFrame(high - low)
    tr2 = pd.DataFrame(abs(high - close.shift()))
    tr3 = pd.DataFrame(abs(low - close.shift()))
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    atr = tr.rolling(period).mean()
    
    # Calculate SuperTrend
    upper_band = (high + low) / 2 + multiplier * atr
    lower_band = (high + low) / 2 - multiplier * atr
    
    super_trend = pd.Series(0.0, index=df.index)
    direction = pd.Series(1, index=df.index)
    
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

# Calculate Chandelier Exit indicator
def calculate_chandelier_exit(df: pd.DataFrame, period: int = 22, multiplier: int = 3) -> pd.DataFrame:
    high = df['High']
    low = df['Low']
    close = df['Close']
    
    # Calculate ATR
    tr1 = pd.DataFrame(high - low)
    tr2 = pd.DataFrame(abs(high - close.shift()))
    tr3 = pd.DataFrame(abs(low - close.shift()))
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

# Get stock data with caching and retry logic
def get_stock_data(symbol: str, interval: str = '30m', period: str = '7d', retry_count: int = 3) -> Optional[pd.DataFrame]:
    # Check cache first
    cache_file = os.path.join(DATA_CACHE_DIR, f"{symbol}_{interval}.csv")
    
    # Use cached data if valid
    if is_cache_valid(symbol, interval):
        try:
            df = pd.read_csv(cache_file, index_col=0, parse_dates=True)
            logger.info(f"Using cached data for {symbol}")
            return df
        except Exception as e:
            logger.warning(f"Error reading cache for {symbol}: {e}")
    
    # Browser-like headers to avoid detection
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    
    # Try different ticker formats and methods
    symbol_variants = [
        symbol,                     # Original format (e.g., "RELIANCE.NS")
        symbol.replace(".NS", ""),  # Without suffix (e.g., "RELIANCE")
        f"{symbol.split('.')[0]}.BO" if ".NS" in symbol else symbol  # BSE alternative
    ]
    
    for variant in symbol_variants:
        for attempt in range(retry_count):
            try:
                # Add random delay to avoid pattern detection
                time.sleep(random.uniform(2.0, 5.0))
                
                logger.info(f"Downloading data for {variant} (attempt {attempt+1}/{retry_count})")
                
                # Method 1: Use yfinance with custom headers
                ticker = yf.Ticker(variant)
                ticker.session.headers.update(headers)  # Set custom headers
                
                # Try different intervals if the standard one fails
                intervals = [interval, '1h', '1d'] if attempt > 0 else [interval]
                periods = [period, '5d', '30d'] if attempt > 0 else [period]
                
                for i in intervals:
                    for p in periods:
                        try:
                            data = ticker.history(interval=i, period=p)
                            if not data.empty and len(data) > 5:
                                logger.info(f"Successfully downloaded data for {variant} using interval={i}, period={p}")
                                # Cache the data
                                data.to_csv(cache_file)
                                return data
                        except Exception:
                            continue
                
                logger.warning(f"No valid data found for {variant} on attempt {attempt+1}")
                
            except json.JSONDecodeError:
                logger.warning(f"JSONDecodeError for {variant} - likely being rate limited")
                time.sleep(10 * (attempt + 1))  # Much longer delay on JSON errors
                
            except Exception as e:
                logger.error(f"Error fetching data for {variant}: {str(e)}")
                time.sleep(5 * (attempt + 1))
    
    # If we get here, all attempts failed
    logger.error(f"Failed to retrieve data for {symbol} after all attempts")
    return None

# Generate trading signals
def generate_signals(symbol: str) -> Optional[Dict]:
    try:
        # Get stock data
        df = get_stock_data(symbol)
        if df is None or len(df) < 25:
            return None
        
        # Calculate indicators
        df = calculate_supertrend(df)
        df = calculate_chandelier_exit(df)
        
        # Check for signals in the latest candle
        latest = df.iloc[-1]
        previous = df.iloc[-2]
        
        signal = None
        reason = []
        
        # SuperTrend signal
        if previous['SuperTrend_Direction'] == -1 and latest['SuperTrend_Direction'] == 1:
            signal = "BUY"
            reason.append("SuperTrend turned bullish")
        elif previous['SuperTrend_Direction'] == 1 and latest['SuperTrend_Direction'] == -1:
            signal = "SELL"
            reason.append("SuperTrend turned bearish")
            
        # Chandelier Exit signal
        if previous['ChandelierExitLong'] == False and latest['ChandelierExitLong'] == True:
            if signal != "SELL":  # Avoid conflicting signals
                signal = "BUY"
                reason.append("Chandelier Exit Long signal")
        elif previous['ChandelierExitShort'] == False and latest['ChandelierExitShort'] == True:
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

# Send Telegram message
def send_telegram_message(message: str) -> bool:
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        payload = {
            'chat_id': TELEGRAM_CHAT_ID,
            'text': message,
            'parse_mode': 'Markdown'
        }
        response = requests.post(url, data=payload)
        if response.status_code == 200:
            logger.info("Telegram message sent successfully")
            return True
        else:
            logger.error(f"Failed to send Telegram message: {response.status_code} - {response.text}")
            return False
    except Exception as e:
        logger.error(f"Error sending Telegram message: {e}")
        return False

# Send WhatsApp message (This is a placeholder - you'll need to integrate with a proper WhatsApp API)
def send_whatsapp_message(message: str) -> bool:
    try:
        # This is a placeholder for WhatsApp Business API integration
        # In a real implementation, you would use a proper WhatsApp Business API
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
           f"*Stock:* {signal['symbol'].replace('.NS', '')}\n" \
           f"*Price:* ₹{signal['price']:.2f}\n" \
           f"*Time:* {signal['time']}\n" \
           f"*Reason:* {signal['reason']}\n\n" \
           f"*Indicators:* SuperTrend & Chandelier Exit (30m)"

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

# Process all stocks for signals with batching
def process_stocks():
    logger.info("Starting stock analysis...")
    stocks = load_nifty500_stocks()
    signals_found = False
    
    # Process in smaller batches to avoid rate limiting
    batch_size = 20
    for i in range(0, len(stocks), batch_size):
        batch = stocks[i:i+batch_size]
        logger.info(f"Processing batch {i//batch_size + 1}/{(len(stocks)//batch_size) + 1} ({len(batch)} stocks)")
        
        for symbol in batch:
            try:
                signal = generate_signals(symbol)
                if signal:
                    signals_found = True
                    message = format_signal_message(signal)
                    logger.info(f"Signal found for {symbol}: {signal['signal']}")
                    
                    # Send notifications
                    send_telegram_message(message)
                    send_whatsapp_message(message.replace('*', ''))  # Remove markdown for WhatsApp
                
                # More substantial rate limiting
                time.sleep(random.uniform(2, 4))
            except Exception as e:
                logger.error(f"Error processing {symbol}: {e}")
                time.sleep(5)  # Longer delay after error
        
        # Add substantial delay between batches
        logger.info(f"Batch {i//batch_size + 1} complete, waiting before next batch")
        time.sleep(random.uniform(15, 30))
    
    if not signals_found:
        logger.info("No signals found in this scan")
        # Optionally send a "no signals" notification
        # send_telegram_message("Scan complete - No trading signals found at this time.")

# Validate data availability for all stocks
def validate_stock_list():
    logger.info("Validating data availability for stock list...")
    stocks = load_nifty500_stocks()
    valid_stocks = []
    
    for symbol in stocks:
        try:
            df = get_stock_data(symbol, retry_count=1)
            if df is not None and not df.empty:
                valid_stocks.append(symbol)
                logger.info(f"✅ {symbol}: Data available")
            else:
                logger.warning(f"❌ {symbol}: No data available")
            time.sleep(random.uniform(1, 2))
        except Exception as e:
            logger.error(f"Error validating {symbol}: {e}")
            time.sleep(3)
    
    # Save valid stocks list
    with open("valid_stocks.json", "w") as f:
        json.dump(valid_stocks, f)
    
    logger.info(f"Validation complete. Found {len(valid_stocks)}/{len(stocks)} valid stocks.")
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
                os.remove(filepath)
                count += 1
    
    logger.info(f"Removed {count} expired cache files")

# Main function to schedule jobs
def main():
    logger.info("Starting Nifty 500 Stock Trading Signal Bot")
    
    # Clean up any expired cache files
    cleanup_cache()
    
    # Validate stock list on startup (optional, uncomment if needed)
    # validate_stock_list()
    
    # Test with a known good stock for debugging (optional)
    # test_single_stock("RELIANCE.NS")
    
    # Run immediately once at startup
    process_stocks()
    
    # Schedule cache cleanup every 4 hours
    schedule.every(4).hours.do(cleanup_cache)
    
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
    
    logger.info("Bot scheduled successfully")
    
    # Keep the script running
    while True:
        schedule.run_pending()
        time.sleep(60)  # Check every minute

if __name__ == "__main__":
    main()
