from pymongo import MongoClient

from .sources import source

client = MongoClient('localhost', 27017)
db = client.quotes_db

#Collections

COLLECTIONS = {
    'open': db.open,
    'high': db.high,
    'low': db.low,
    'close': db.close,
    'adj_close': db.adj_close,
    'volume': db.volume,
    'dividend': db.dividend,
    'split_coeff': db.split_coeff,
}


def get_single(self, symbol):
    return collection.find_one(
        {'symbol': symbol}, projection={'_id': False, 'quotes': True}
    )

def get_multiple(symbols):
    docs = collection.find({'symbol': {'$in': symbols}})
    return [doc for doc in docs]

def get_series(symbol, collection='adj_close'):
    pass

def write_series(symbol, data, collection_name):
    doc = {
        'symbol': symbol,
        'quotes': data,
    }
    collection = collections[collection_name]
    collection.replace_one(
        {'symbol': symbol},
        doc,
        upsert=True
    )

def get_for_date(date, symbols=[]):
    pass

def write_single(data, symbol):

    doc = {
        'symbol': symbol,
        'quotes': data
    }

    collection.replace_one(
        {'symbol': symbol},
        doc,
        upsert=True
    )

def get_symbols():
    return [doc['symbol'] for doc in collection.find()]

def update_single(symbol):
    data = source.load_single(symbol)
    self.write_single(data, symbol)

def update_all():
    for symbols in self.symbols():
        self.update_single(ticker)
