import os
import asyncio
import statistics
import pandas as pd
import examples.historic_async as ha
import alpaca_trade_api as tradeapi
from datetime import timedelta
from sklearn.pipeline import Pipeline


class ML:
    def __init__(self, key, secret, url):
        self.api = tradeapi(key, secret, url, 'v2')
        self.assets = self.api.list_assets()
        self.assets = [asset for asset in self.assets if asset.tradable]
        self.assets = [asset for asset in self.assets if asset.fractionable]
        # close all positions
        self.api.close_all_positions()


