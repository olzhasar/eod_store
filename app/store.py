import os
import time
import pickle
import pandas as pd

from flask import Response

from sources import source
from exceptions import APIError
from factories import create_application, create_celery

celery = create_celery(create_application())

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, 'data/')
SYMBOLS_FILE = os.path.join(DATA_DIR, 'symbols')

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

VALID_FIELDS = list(FIELD_MAPPINGS.keys())


def get_symbols():
    try:
        with open(SYMBOLS_FILE, 'rb') as f:
            symbols = pickle.load(f)
    except EOFError:
        symbols = []
    return symbols


def in_database(symbol):
    symbols = get_symbols()
    return symbol in symbols


def overwrite_symbols(symbols):
    with open(SYMBOLS_FILE, 'wb') as f:
        pickle.dump(symbols, f)


def add_symbols(symbols):
    existing = get_symbols()
    for symbol in symbols:
        if symbol in existing:
            raise APIError("Symbol %s already exists in database" % symbol)
        if not source.has_data(symbol):
            raise APIError("No data available for symbol: %s" % symbol)
        existing.append(symbol)
    overwrite_symbols(existing)
    return "Successfully added symbols %s to database" % symbols


def remove_symbols(symbols):
    existing = get_symbols()
    for symbol in symbols:
        if symbol not in existing:
            raise APIError("Symbol %s not found in database" % symbol)
        existing.remove(symbol)
        # Delete all files in DATA_DIR
    overwrite_symbols(existing)
    return "Successfully deleted all data for symbols %s" % symbols


@celery.task
def update_background(symbols):
    for i, symbol in enumerate(symbols, 1):
        df = source.load_single(symbol)
        df.to_hdf(
            os.path.join(DATA_DIR, symbol),
            key=symbol,
            mode='w'
        )
        print("Successfully updated data for %s" % symbol)
        if i < len(symbols):
            time.sleep(20)


@celery.task
def print_something():
    print("SOMETHING")


def update_all():
    symbols = get_symbols()
    update_background.delay(symbols)
    return 'Started updating data for %s' % symbols


def query_single(symbol, fields="ohlcavds"):
    if not in_database(symbol):
        raise APIError("No data for symbol: %s in database" % symbol)
    columns = []
    for field in fields:
        if field not in VALID_FIELDS:
            raise APIError("Invalid field %s" % field)
        columns.append(FIELD_MAPPINGS[field])
    try:
        df = pd.read_hdf(os.path.join(DATA_DIR, symbol), symbol)
    except FileNotFoundError:
        raise APIError("No file for %s" % symbol)
    response = Response(
        response=df[columns].to_json(),
        status=200,
        mimetype="application/json"
    )
    return response


def query_batch(symbols, field):
    if field not in list(FIELD_MAPPINGS.values()):
        raise APIError("Invalid field %s" % field)
    data = []
    for symbol in symbols:
        try:
            df = pd.read_hdf(os.path.join(DATA_DIR, symbol), symbol)
        except FileNotFoundError:
            raise APIError("No file for %s" % symbol)
        data.append(df[field])
    data = pd.concat(data, axis=1, keys=symbols, sort=True)
    response = Response(
        response=data.to_json(),
        status=200,
        mimetype="application/json"
    )
    return response
