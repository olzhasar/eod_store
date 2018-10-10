#!/usr/bin/env python
from flask import Flask, jsonify
from mongo_quotes.storage import get_symbols

app = Flask(__name__)


@app.route('/')
def home():
    return jsonify({'symbols': get_symbols()})


if __name__ == "__main__":
    app.run(debug=True)
