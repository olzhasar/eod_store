#!/usr/bin/env python
from flask import Flask, jsonify, request
from exceptions import APIError
import store

app = Flask(__name__)


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
        try:
            symbol = request.args['symbol']
        except KeyError:
            raise APIError("You must provide a valid symbol")
        if request.method == 'POST':
            return store.add_symbol(symbol)
        elif request.method == 'DELETE':
            return store.remove_symbol(symbol)


@app.route('/query/<symbol>', methods=['GET', ])
def query_single(symbol):
    try:
        fields = request.args['fields']
    except KeyError:
        fields = 'ohlcavds'
    return jsonify(store.query_single(symbol, fields))


@app.route('/query_batch', methods=['GET', ])
def query_batch():
    symbols = request.args.getlist('symbol')
    if not symbols:
        raise APIError("No symbols provided")
    try:
        fields = request.args['fields']
    except KeyError:
        fields = 'ohlcavds'
    return "Symbols: %s" % symbols


@app.route('/daily/<collection>/<symbol>')
def get_series(collection, symbol):
    return jsonify(store.get_series(symbol, collection))


@app.route('/quote/<collection>/<symbol>')
def get_quote(collection, symbol):
    return jsonify(store.get_quote(symbol, collection))


@app.route('/quote/<collection>/<symbol>/<date>')
def get_quote_date(collection, symbol, date):
    return jsonify(store.get_quote(symbol, collection, date))


@app.route('/update/')
def update_all():
    return store.update_all()


if __name__ == "__main__":
    app.run(host='0.0.0.0', debug=True)
