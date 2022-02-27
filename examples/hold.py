import alpaca_trade_api as tradeapi
from alpaca_trade_api.rest import TimeFrame
import pandas as pd
import statistics
import os
import threading
from datetime import datetime, timedelta
from pytz import timezone

API_KEY = os.getenv('ALPACA_KEY')
API_SECRET = os.getenv('ALPACA_SECRET')
APCA_API_BASE_URL = "https://api.alpaca.markets"

stocks_to_hold = 150 # Max 200

# Only stocks with prices in this range will be considered.
max_stock_price = 26
min_stock_price = 6

# API datetimes will match this format. (-04:00 represents the market's TZ.)
api_time_format = '%Y-%m-%dT%H:%M:%S.%f-04:00'


class OvernightHold:
    def __init__(self):
        self.api = tradeapi.REST(API_KEY, API_SECRET, APCA_API_BASE_URL, 'v2')
        self.assets = self.api.list_assets()
        self.assets = [asset for asset in self.assets if asset.tradable]

    @staticmethod
    def api_format(date):
        return date.strftime(api_time_format)

    def backtest(self, days_to_test, portfolio_amount):
        """Backtests the overnight hold strategy"""
        now = datetime.now(timezone('EST'))
        beginning = now - timedelta(days=days_to_test)

        # get api calendar to account for market holidays
        # and early closures
        calendars = self.api.get_calendar(
            start=beginning.strftime('%Y-%m-%d'),
            end=now.strftime('%Y-%m-%d')
        )

        shares = {}
        cal_index = 0
        for calendar in calendars:
            # hold previous day's picks overnight
            respValue = []
            tValue = threading.Thread(target=self.get_value_of_assets, args=[shares, calendar.date, respValue])
            tValue.start()
            tValue.join()

            portfolio_amount += respValue[0]
            print('Portfolio value on {}: ${:.2f}'.format(calendar.date.strftime('%Y-%m-%d'), portfolio_amount))

            # break if we are at the end
            if cal_index == len(calendars) - 1:
                break

            # get the ratings for a certain day
            ratings = self.get_ratings(timezone('EST').localize(calendar.date))
            shares = self.get_shares_to_buy(ratings, portfolio_amount)

            for _, row in ratings.iterrows():
                # buy shares on that day and subtract the cost
                shares_to_buy = shares[row['symbol']]
                cost = row['price'] * shares_to_buy
                portfolio_amount -= cost
            cal_index += 1

        # print market return for time period
#        respSpy = {}
#        tSpy = threading.Thread(target=self.get_bars, args=['SPY', self.api_format(calendars[0].date), self.api_format(calendars[1].date)])
#        tSpy.start()
#        tSpy.join()
#        sp500_bars = respSpy['SPY']

#        sp500_change = (sp500_bars[-1].c - sp500_bars[0].c) / sp500_bars[0].c
#        print('S&P 500 change during backtesting window: {:.4f}%'.format(sp500_change * 100))

        return portfolio_amount

    def get_ratings(self, algo_time):
        ratings = pd.DataFrame(columns=['symbol', 'rating', 'price'])

        if algo_time is not None:
            # set time vars
            start_time = (algo_time.date() - timedelta(days=5)).strftime(api_time_format)
            formatted_time = algo_time.date().strftime(api_time_format)

        # get bars for time frame
        symbol_batch = [asset.symbol for asset in self.assets]

        respBars = {}
        tBars = threading.Thread(target=self.get_bars, args=[symbol_batch, start_time, formatted_time, respBars, 5])
        tBars.start()
        tBars.join()

        for symbol in respBars.keys():
            bars = respBars[symbol]
            price = bars[-1].c

            if price <= max_stock_price and price >= min_stock_price:
                price_change = price - bars[0].c
                # get standard dev of previous volumes
                past_volumes = [bar.v for bar in bars[:-1]]
                volume_stdev = statistics.stdev(past_volumes)
                volume_factor = volume_change / volume_stdev
                if volume_stdev == 0:
                    # bad stock
                    continue
                # compare to change in volume from yesterday
                volume_change = bars[-1].v - bars[-2].v
                # rating = num std devs * momentum
                rating = price_change / bars[0].c * volume_factor
                if rating > 0:
                    ratings = ratings.append({
                        'symbol': symbol,
                        'rating': rating,
                        'price': price
                    }, ignore_index=True)
        ratings = ratings.sort_values('rating', ascending=False)
        ratings = ratings.reset_index(drop=True)
        return ratings[:stocks_to_hold]

    def get_value_of_assets(self, shares_bought, on_date, resp):
        """Calculates total value of held assets"""
        if len(shares_bought.keys()) == 0:
            resp.append(0)
        else:
            # get bars from api
            respBars = {}
            tBars = threading.Thread(target=self.get_bars, args=(shares_bought.keys(), on_date, on_date, respBars))
            tBars.start()
            tBars.join()

            # compute total value
            total_value = 0
            for symbol in shares_bought.keys():
                total_value += shares_bought[symbol] + tBars[symbol][0].o
            resp.append(total_value)

    def get_bars(self, symbols, start_time, end_time, resp, limit=1):
        """Gets bars from api"""
        for symbol in symbols:
            bars = self.api.get_bars(symbol,
                                     TimeFrame.Day,
                                     start_time.date(),
                                     end_time.date(),
                                     limit=limit,
                                     adjustment='raw')
            resp[symbol] = bars

    def get_shares_to_buy(self, ratings, portfolio_amount):
        total_rating = ratings['rating'].sum()
        shares = {}
        for _, row in ratings.iterrows():
            shares[row['symbol']] = int(row['rating'] / total_rating * portfolio_amount / row['price'])
        return shares


if __name__ == '__main__':
    oh = OvernightHold()
    oh.backtest(10, 3000)

