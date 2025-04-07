# Nifty 500 Stock Trading Signal Bot

This bot analyzes Nifty 500 stocks on a 30-minute timeframe using SuperTrend and Chandelier Exit indicators and sends trading signals to Telegram and WhatsApp.

## Features

- Analyzes all Nifty 500 stocks
- Uses SuperTrend and Chandelier Exit indicators for signal generation
- 30-minute timeframe analysis
- Sends real-time alerts to Telegram and WhatsApp
- Runs automatically during market hours

## Prerequisites

- Python 3.8+
- Docker (optional)
- Telegram Bot Token
- WhatsApp Business API access (or an alternative method to send WhatsApp messages)

## Setup Instructions

### 1. Clone the Repository

```bash
git clone https://github.com/yourusername/nifty500-trading-bot.git
cd nifty500-trading-bot
```

### 2. Environment Setup

#### Option 1: Local Installation

Create a virtual environment and install dependencies:

```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -r requirements.txt
```

#### Option 2: Docker Installation

Build and run using Docker Compose:

```bash
docker-compose up -d
```

### 3. Configuration

The bot uses environment variables for configuration. These are already set in the docker-compose.yml file, but you can modify them if needed:

- `TELEGRAM_TOKEN`: Your Telegram bot token
- `TELEGRAM_CHAT_ID`: Your Telegram chat ID
- `WHATSAPP_NUMBER`: Your WhatsApp number

### 4. WhatsApp API Integration

**Note:** The current implementation includes a placeholder for WhatsApp integration. To fully implement WhatsApp messaging:

1. Sign up for WhatsApp Business API or use a third-party service like Twilio or MessageBird
2. Update the `send_whatsapp_message()` function in main.py with your API details

### 5. Running the Bot

#### Local Execution:

```bash
python main.py
```

#### Docker Execution:

```bash
docker-compose up -d
```

## How It Works

1. The bot fetches 30-minute candlestick data for all Nifty 500 stocks
2. Calculates SuperTrend and Chandelier Exit indicators
3. Identifies buy/sell signals based on indicator crossovers
4. Sends formatted alerts to Telegram and WhatsApp
5. Runs every 30 minutes during market hours (9:15 AM to 3:30 PM, Monday to Friday)

## Signal Logic

- **Buy Signal**:
  - SuperTrend turns from bearish to bullish, OR
  - Chandelier Exit Long signal is triggered

- **Sell Signal**:
  - SuperTrend turns from bullish to bearish, OR
  - Chandelier Exit Short signal is triggered

## GitHub Actions CI/CD

The repository includes GitHub Actions workflows for automated testing and deployment. See `.github/workflows` directory for details.

## Customization

You can customize the bot parameters by modifying the following variables in `main.py`:

- SuperTrend period and multiplier
- Chandelier Exit period and multiplier
- Scanning interval
- Market hours

## Logging

Logs are stored in the `logs` directory. The log file contains detailed information about bot execution, signal generation, and any errors encountered.

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Disclaimer

This trading bot is provided for educational and informational purposes only. Trading in financial markets involves significant risk, and past performance is not indicative of future results. Always conduct your own research before making investment decisions.
