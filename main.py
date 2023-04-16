import alpaca_trade_api as tradeapi
from alpaca_trade_api import REST
import pandas as pd
import ta
import time

# Set up Alpaca API
api_key
api_secret
base_url = 'https://paper-api.alpaca.markets'
api = tradeapi.REST(api_key, api_secret, base_url, api_version='v2')

# Get S&P500 stock symbols
assets = api.list_assets(status='active', asset_class='us_equity')
filtered_assets = [asset for asset in assets if asset.exchange == 'NYSE']
symbols = [asset.symbol for asset in filtered_assets]
#symbols = ['AAPL', 'MSFT', 'AMZN']

# Define RSI parameters
rsi_period = 14
rsi_buy_threshold = 32

# Define trade parameters
quantity = 1

# Create position entry timestamps dictionary
position_entry_timestamps = {}

# Set up trading loop
while True:

    # Get latest price data for S&P500 stocks
    prices = {}
    #symbols = ['AAPL', 'MSFT', 'AMZN']

    for symbol in symbols:
        try:
            bars = api.get_bars(timeframe='1D', symbol=symbol, limit=rsi_period + 1)
            if bars:
                close_prices = [bar.c for bar in bars]
                prices[symbol] = close_prices
        except Exception as e:
            print(f"Error fetching data for {symbol}: {e}")
        time.sleep(3)  # Add a delay between API calls

    # Calculate RSI for each stock
    rsis = pd.DataFrame(index=symbols, columns=['rsi'])
    for symbol in symbols:
        if symbol in prices:
            close_prices = pd.Series(prices[symbol])
            if len(close_prices) >= rsi_period:
                rsi = ta.rsi(close_prices, rsi_period)[-1]
                rsis.at[symbol, 'rsi'] = ta.rsi(close_prices, rsi_period)[-1]

    # Buy shares for stocks with RSI crossing above buy threshold

    for symbol, rsi in rsis.itertuples():
        if rsi is not None and rsi >= rsi_buy_threshold:
            print(f"{symbol} RSI crossed threshold: {rsi}")
            try:
                position = api.get_position(symbol)
                if position.qty == '0':
                    order = api.submit_order(symbol=symbol, qty=quantity, side='buy', type='market',
                                             time_in_force='gtc')
                    position_entry_timestamps[symbol] = pd.Timestamp.now(tz='America/New_York')
            except tradeapi.rest.APIError as e:
                if e.status_code == 404:  # Position not found
                    order = api.submit_order(symbol=symbol, qty=quantity, side='buy', type='market',
                                             time_in_force='gtc')
                    position_entry_timestamps[symbol] = pd.Timestamp.now(tz='America/New_York')

    # Sell shares for stocks held for 41 days
    positions = api.list_positions()
    for position in positions:
        if position.symbol in symbols and position.side == 'long':
            if position.symbol in position_entry_timestamps:
                if (pd.Timestamp.now(tz='America/New_York') - position_entry_timestamps[position.symbol]) > pd.Timedelta(days=41):
                    api.submit_order(symbol=position.symbol, qty=abs(int(position.qty)), side='sell', type='market',
                                     time_in_force='gtc')

    # Wait for next trading day
    clock = api.get_clock()
    next_open = clock.next_open
    current_time = clock.timestamp

    # Calculate time until the next market open and add a small buffer (e.g., 60 seconds)
    wait_time = (next_open - current_time).total_seconds() + 60

    # Sleep until the next trading day
    time.sleep(wait_time)
