import os
from alpha_vantage.timeseries import TimeSeries


class AbstractSource():

    """
    Abstract class for Source. This class is responsible for loading
    market data from external sources. Sources can be clients of API's like
    AlphaVantage, Quandl, or get market data any other way (e.g. web scraping)
    """

    def load_single(symbol):
        """
        This function should return dataframe, containing ohlc, volume, split
        and dividends data for specified symbol
        """
        raise NotImplementedError

    def load_batch(symbols):
        """
        This function takes list of symbols and should return list of
        dataframes, containing data for each specified symbol
        """
        raise NotImplementedError

    def check_symbol(symbol):
        """
        This function checks whether data for given ticket exists in this
        source and returns boolean
        """
        raise NotImplementedError


class AlphaVantage(AbstractSource):

    def __init__(self, output_format='pandas'):
        self.ts = TimeSeries(
            key=os.environ['ALPHA_VANTAGE_KEY'],
            output_format=output_format
        )

    def load_single(self, symbol, format='full'):
        return self.ts.get_daily_adjusted(symbol, format)[0]

    def check_symbol(self, symbol):
        try:
            self.get_series(symbol, format='compact')
        except ValueError:
            return False
        return True


source = AlphaVantage()
