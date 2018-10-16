import os
from alpha_vantage.timeseries import TimeSeries


class Feed():

    """
    Abstract class for Feed. This class is responsible for loading
    market data from external feeds. Feeds can be clients of API's like
    AlphaVantage, Quandl, or get market data any other way (e.g. web scraping)
    """

    def load_single(symbol):
        """
        This function should return pandas dataframe, containing ohlc, volume,
        split and dividends data for specified symbol
        """
        raise NotImplementedError

    def has_data(symbol):
        """
        This function checks whether data for given ticket exists in this
        feed and returns boolean
        """
        raise NotImplementedError


class AlphaVantage(Feed):

    def __init__(self, output_format='pandas'):
        self.ts = TimeSeries(
            key=os.environ['ALPHA_VANTAGE_KEY'],
            output_format=output_format
        )
        self.COLUMNS_DICT = {
            '1. open': 'open',
            '2. high': 'high',
            '3. low': 'low',
            '4. close': 'close',
            '5. adjusted close': 'adj_close',
            '6. volume': 'volume',
            '7. dividend amount': 'dividend',
            '8. split coefficient': 'split_coeff',
        }

    def load_single(self, symbol, format='full'):
        df = self.ts.get_daily_adjusted(symbol, format)[0]
        df.rename(columns=self.COLUMNS_DICT, inplace=True)
        return df

    def has_data(self, symbol):
        try:
            self.load_single(symbol, format='compact')
        except ValueError:
            return False
        return True


feed = AlphaVantage()
