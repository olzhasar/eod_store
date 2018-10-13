import time
from pymongo import MongoClient
from bson.json_util import dumps

from sources import source
from exceptions import APIError

client = MongoClient('mongodb://datastore:27017/mongo_quotes')
db = client.quotes_db.quotes

# Dictionary of collections in our database

FIELD_MAPPINGS = {
    'o': 'open',
    'h': 'high',
    'l': 'low',
    'c': 'close',
    'a': 'adj_close',
    'v': 'volume',
    'd': 'dividend',
    's': 'split_coeff'
}

VALID_FIELDS = list(FIELD_MAPPINGS.values())


def get_symbols():
    return [doc['symbol'] for doc in db.find()]


def in_database(symbol):
    if db.find_one({'symbol': symbol}):
        return True
    return False


def add_symbols(symbols):
    for symbol in symbols:
        if not source.symbol_has_data(symbol):
            raise("No data available for symbol: %s" % symbol)
        if in_database(symbol):
            raise APIError("Symbol %s already exists in database" % symbol)
        db.insert_one({'symbol': symbol})
    return "Successfully added symbols %s to database" % symbols


def remove_symbols(symbols):
    for symbol in symbols:
        if not in_database(symbol):
            raise APIError("Symbol %s not found in database" % symbol)
        db.delete_one({'symbol': symbol})
    return "Successfully deleted all data for symbols %s" % symbols


def update_single(symbol):
    data = source.load_single(symbol)
    doc = {'symbol': symbol, **data}
    db.replace_one(
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


def query_single(symbol, fields="ohlcavds"):
    if not in_database(symbol):
        raise APIError("No data for symbol: %s in database" % symbol)
    projection_dict = {}
    for f in fields:
        field_name = FIELD_MAPPINGS[f]
        projection_dict[field_name] = True

    return db.find_one(
        {'symbol': symbol},
        projection={'_id': False, **projection_dict}
    )


def query_batch(symbols, field):
    if field not in VALID_FIELDS:
        raise APIError("Invalid field: %s" % field)
    return dumps(db.find(
        {'symbol': {"$in": symbols}},
        projection={field: {"$slice": 5}, '_id': 0, 'symbol': 1}
    ))


def get_quote(symbol, date=None):
    pass
