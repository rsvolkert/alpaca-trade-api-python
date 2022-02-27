import os
import asyncio
import statistics
import pandas as pd
import examples.historic_async as ha
import alpaca_trade_api as tradeapi
from datetime import timedelta

stocks_to_hold = 150 # Max 200

# Only stocks with prices in this range will be considered.
max_stock_price = 26
min_stock_price = 6


class Crossover:
    def __init__(self, key, secret, url):
        self.api = tradeapi.REST(key, secret, url, 'v2')
        self.assets = self.api.list_assets()
        self.assets = [asset for asset in self.assets if asset.tradable]
        self.assets = [asset for asset in self.assets if asset.fractionable]
        # close all positions
        self.api.close_all_positions()
        print('Closed positions')

    @staticmethod
    def _get_rating(bar, price):
        price_change = price - bar[-20:].close[0]
        # calculate std of past volumes
        past_volumes = bar[:-1][-19:].volume.to_list()
        volume_stdev = statistics.stdev(past_volumes)
        # data might be bad quality
        assert volume_stdev != 0, 'bad quality'
        # compare to change since yesterday
        volume_change = bar[-1:].volume[0] - bar[-2:].volume[0]
        volume_factor = volume_change / volume_stdev
        # calculate rating
        rating = price_change / bar[-20:].close[0] * volume_factor

        return rating

    def run(self):
        buy = pd.DataFrame()

        symbols = [asset.symbol for asset in self.assets]

        bars = asyncio.run(ha.main(symbols,
                           (pd.Timestamp('now', tz='America/New_York') - timedelta(days=40)).date().isoformat(),
                           pd.Timestamp('now', tz='America/New_York').date().isoformat()))

        for symbol in bars.keys():
            if isinstance(bars[symbol], Exception):
                continue
            bar = bars[symbol][1]
            # check if we got no data
            if bar.empty:
                continue
            # get price
            price = bar[-1:].close[0]
            # move on if price not in desired range
            if price > max_stock_price or price < min_stock_price:
                continue
            # get moving averages
            ma20 = bar[-20:].close.mean()
            ma10 = bar[-10:].close.mean()
            # see if there is crossover
            if ma10 > ma20:
                try:
                    rating = self._get_rating(bar, price)
                    buy = buy.append({'symbol': symbol, 'rating': rating}, ignore_index=True)
                except AssertionError:
                    continue
        buy = buy.sort_values('rating', ascending=False)
        buy.reset_index(inplace=True, drop=True)
        buy = buy[:stocks_to_hold]

        # buy same $ amount of each
        # truncates to two decimal places
        notional = int((float(self.api.get_account().buying_power) / buy.shape[0]) * 100) / 100

        for symbol in buy.symbol.to_list():
            self.api.submit_order(symbol,
                                  side='buy',
                                  notional=notional)
            print(f'Bought ${notional} worth of {symbol}')


if __name__ == '__main__':
    key = os.getenv('PAPER_KEY')
    secret = os.getenv('PAPER_SECRET')
    url = 'https://paper-api.alpaca.markets'

    crossover = Crossover(key, secret, url)
    crossover.run()

