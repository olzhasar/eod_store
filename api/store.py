import os
import json
import pandas as pd

from flask import Blueprint, request, Response, jsonify, current_app as app

from .exceptions import APIError

bp = Blueprint('store', __name__)

FIELDS = [
    'open',
    'high',
    'low',
    'close',
    'adj_close',
    'volume',
    'dividend',
    'split_coeff'
]
FIELD_LETTERS = [field[0] for field in FIELDS]
FIELD_MAPPINGS = dict(zip(FIELD_LETTERS, FIELDS))


def list_meta():
    with open(app.config['SYMBOLS_FILE'], 'rb') as fp:
        data = json.load(fp)
    return data


def list_symbols():
    symbols = list(list_meta().keys())
    return symbols


def read_data(symbol):
    try:
        df = pd.read_pickle(
            os.path.join(app.config['DATA_DIR'], symbol)
        )
    except FileNotFoundError:
        raise APIError(
            message='No data for symbol %s' % symbol
        )
    return df


@bp.route('/meta', methods=['POST'])
def meta():
    meta = list_meta()
    if request.is_json:
        symbols = request.json.get("symbols", None)
        if symbols is not None:
            try:
                meta = {s: meta[s] for s in symbols}
            except KeyError:
                raise APIError(
                    message="Invalid symbols specified"
                )
    return jsonify(meta)


@bp.route('/update', methods=['PUT'])
def update():
    from tasks import update_data
    symbols = None
    if request.is_json:
        symbols = request.json.get("symbols")
    update_data.delay(symbols or list_symbols())
    return "Started updating data"


@bp.route('/query_single', methods=['POST'])
def query_single():
    try:
        symbol = request.json['symbol']
    except KeyError:
        raise APIError(
            message='Symbol must be provided'
        )
    fields = request.json.get('fields', 'ohlc')
    if not set([f for f in fields]).issubset(FIELD_LETTERS):
        raise APIError(
            message='Invalid fields specified'
        )
    start = request.json.get('start', None)
    end = request.json.get('end', None)
    limit = request.json.get('limit', None)
    columns = [FIELD_MAPPINGS[f] for f in fields]
    data = read_data(symbol)[columns].loc[start:end]
    if limit is not None:
        data = data[-int(limit):]

    response = Response(
        response=data.to_json(),
        mimetype='application/json'
    )
    return response


@bp.route('/query_multiple', methods=['POST'])
def query_multiple():
    try:
        symbols = request.json['symbols']
    except KeyError:
        raise APIError(
            message='Symbol list must be provided'
        )
    field = request.json.get('field', 'adj_close')
    if field not in FIELDS:
        raise APIError(
            message='Invalid field specified'
        )
    start = request.json.get('start', None)
    end = request.json.get('end', None)
    data = [read_data(s)[field].loc[start:end] for s in symbols]
    data = pd.concat(data, axis=1, keys=symbols, sort=True)

    response = Response(
        response=data.to_json(),
        mimetype='application/json'
    )
    return response
