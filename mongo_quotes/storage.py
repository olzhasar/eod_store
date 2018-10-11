import time
from pymongo import MongoClient

from .sources import source

client = MongoClient('localhost', 27017)
db = client.quotes_db

# Dictionary of collections in our database

COLLECTIONS = {
    # General collection for storing and quickly retrieving all symbols in db:
    'symbols': db.symbols,
    # Specific collections for each type of daily data:
    'open': db.open,
    'high': db.high,
    'low': db.low,
    'close': db.close,
    'adj_close': db.adj_close,
    'volume': db.volume,
    'dividend': db.dividend,
    'split_coeff': db.split_coeff,
}


def get_symbols():
    return [doc['symbol'] for doc in COLLECTIONS['symbols'].find()]


def is_in_database(symbol):
    if COLLECTIONS['symbols'].find_one({'symbol': symbol}):
        return True
    return False


def add_symbol(symbol):
    if not source.symbol_has_data(symbol):
        return "There is no data in source for specified symbol: %s" % symbol
    if is_in_database(symbol):
        return "Symbol %s already exists in database"
    COLLECTIONS['symbols'].insert_one({'symbol': symbol})
    return "Successfully added symbol %s to database" % symbol


def remove_symbol(symbol):
    if is_in_database(symbol):
        for collection in COLLECTIONS:
            COLLECTIONS[collection].delete_one({'symbol': symbol})
        return "Successfully deleted all data for symbol %s" % symbol
    return "Symbol %s not found in database" % symbol


def update_single(symbol):
    df = source.load_single(symbol)
    for column in df.columns:
        doc = {
            'symbol': symbol,
            'quotes': df[column].to_dict(),
        }
        COLLECTIONS[column].replace_one(
            {'symbol': symbol}, doc,
            upsert=True
        )


def update_all():
    symbols = get_symbols()
    for i, symbol in enumerate(symbols, 1):
        update_single(symbol)
        if i < len(symbols):
            time.sleep(20)
    return 'Successfully updated data for %s' % symbols


def get_series(symbol, collection):
    return COLLECTIONS[collection].find_one(
        {'symbol': symbol},
    )['quotes']


def get_batch_series(collection, symbols=None, start_date=None, end_date=None):
    pass


def get_quote(symbol, collection, date=None):
    if date:
        return COLLECTIONS[collection].find_one(
            {
                'symbol': symbol,
            },
            projection={
                '_id': False,
                'quotes.{}'.format(date): True
            }
        )['quotes'][date]
    return COLLECTIONS[collection].find_one(
        {
            'symbol': symbol,
        },
        projection={
            '_id': False,
            'quotes': True,
        },
    )['quotes'].popitem()[1]
