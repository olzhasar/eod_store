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


def get_series(symbol, collection):
    return COLLECTIONS[collection].find_one(
        {'symbol': symbol},
    )['quotes']


def write_series(symbol, data, collection_name):
    doc = {
        'symbol': symbol,
        'quotes': data,
    }
    collection = COLLECTIONS[collection_name]
    collection.replace_one(
        {'symbol': symbol}, doc,
        upsert=True
    )


def get_for_date(date, symbols=[]):
    pass


def update_single(symbol):
    df = source.load_single(symbol)
    for column in df.columns:
        doc = {
            'symbol': symbol,
            'quotes': df[column].to_json(),
        }
        COLLECTIONS[column].replace_one(
            {'symbol': symbol}, doc,
            upsert=True
        )


def update_all():
    for symbols in get_symbols():
        update_single(symbols)
    return 'Updated successfully'
