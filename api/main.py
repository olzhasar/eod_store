#!/usr/bin/env python
import os
import pandas as pd
from jsonrpc.backend.flask import api
from jsonrpc.exceptions import JSONRPCDispatchException

from factories import create_application, create_celery

app = create_application()
celery = create_celery(app)

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


def _csv():
    return pd.read_csv(app.config.SYMBOLS_FILE, header=0)


def get_symbols():
    return _csv.index.tolist()


def exists(symbol):
    return symbol in get_symbols()


app.add_url_rule('/', 'api', api.as_view(), methods=['POST'])


@api.dispatcher.add_method
def query_single(**kwargs):
    try:
        symbol = kwargs['symbol']
    except KeyError:
        raise JSONRPCDispatchException(
            code=-32600,
            message='Symbol must be provided'
        )
    assert isinstance(symbol, str)
    fields = kwargs.get('fields', 'ohlc')
    if not set([f for f in fields]).issubset(VALID_FIELDS):
        raise JSONRPCDispatchException(
            code=-32600,
            message='Invalid fields specified'
        )
    columns = [FIELD_MAPPINGS[f] for f in fields]
    try:
        df = pd.read_hdf(os.path.join(app.config['DATA_DIR'], symbol), symbol)
    except FileNotFoundError:
        raise JSONRPCDispatchException(
            code=-32000,
            message='No data for symbol %s' % symbol
        )
    return df[columns].to_json()


@api.dispatcher.add_method
def query_multiple(**kwargs):
    pass
