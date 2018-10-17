import os
import time
import pickle
import pandas as pd

from flask import Response

from feeds import feed
from exceptions import APIError
from factories import create_application, create_celery

celery = create_celery(create_application())

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, 'data/')
SYMBOLS_FILE = os.path.join(DATA_DIR, 'symbols')


@celery.task
def update_background(symbols):
    for i, symbol in enumerate(symbols, 1):
        df = feed.load_single(symbol)
        df.to_hdf(
            os.path.join(DATA_DIR, symbol),
            key=symbol,
            mode='w'
        )
        print("Successfully updated data for %s" % symbol)
        if i < len(symbols):
            time.sleep(20)


def update_all():
    symbols = get_symbols()
    update_background.delay(symbols)
    return 'Started updating data for %s' % symbols


def query(symbols, fields):

    if not set([f for f in fields]).issubset(VALID_FIELDS):
        raise APIError(
            "Invalid fields requested. Available fields: %s" % VALID_FIELDS,
            400
        )
    columns = [FIELD_MAPPINGS[f] for f in fields]

    existing = get_symbols()
    data = []

    for symbol in symbols:
        if symbol not in existing:
            raise APIError("No data for symbol %s" % symbol, 400)
        try:
            df = pd.read_hdf(os.path.join(DATA_DIR, symbol), symbol)
        except FileNotFoundError:
            raise APIError(
                "Data for %s has not been fetched yet" % symbol, 500
            )
        data.append(df[columns].to_json())

    response = Response(
        response=data,
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
