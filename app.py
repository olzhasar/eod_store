#!/usr/bin/env python
from flask import Flask, jsonify
from mongo_quotes import storage

app = Flask(__name__)


@app.route('/')
@app.route('/symbols/')
def home():
    return jsonify({'symbols': storage.get_symbols()})


@app.route('/symbols/add/<symbol>')
def add_symbol(symbol):
    return storage.add_symbol(symbol)


@app.route('/symbols/remove/<symbol>')
def remove_symbol(symbol):
    return storage.remove_symbol(symbol)


@app.route('/daily/<collection>/<symbol>')
def get_series(collection, symbol):
    return jsonify(storage.get_series(symbol, collection))


@app.route('/update/')
def update_all():
    return storage.update_all()


if __name__ == "__main__":
    app.run(debug=True)
