#!/usr/bin/env python
import os
from flask import Flask, jsonify, request
from celery import Celery
from exceptions import APIError

import store

app = Flask(__name__)


CELERY_BROKER_URL = os.environ.get('CELERY_BROKER_URL',
                                   'redis://localhost:6379')
CELERY_RESULTS_BACKEND = os.environ.get('CELERY_BROKER_URL',
                                        'redis://localhost:6379')

celery = Celery(app.import_name, broker=CELERY_BROKER_URL,
                backend=CELERY_RESULTS_BACKEND)


@app.errorhandler(APIError)
def handle_invalid_usage(error):
    response = jsonify(error.to_dict())
    response.status_code = error.status_code
    return response


@app.route('/', methods=['GET', ])
@app.route('/symbols', methods=['GET', 'POST', 'DELETE'])
def all_symbols():
    if request.method == 'GET':
        return jsonify(store.get_symbols())
    else:
        symbols = request.args.getlist('symbol')
        if not symbols:
            raise APIError("You must provide a valid symbol")
        if request.method == 'POST':
            return store.add_symbols(symbols)
        elif request.method == 'DELETE':
            return store.remove_symbols(symbols)


@app.route('/update/')
def update_all():
    return store.update_all()


@app.route('/query/<symbol>', methods=['GET', ])
def query_single(symbol):
    try:
        fields = request.args['fields']
    except KeyError:
        fields = 'ohlcavds'
    return jsonify(store.query_single(symbol, fields))


@app.route('/<field>', methods=['GET', ])
def query_batch(field):
    symbols = request.args.getlist('symbol')
    if not symbols:
        raise APIError("No symbols provided")
    return store.query_batch(symbols, field)


if __name__ == "__main__":
    app.run(host='0.0.0.0', debug=True)
