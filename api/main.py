#!/usr/bin/env python
from flask import jsonify, request
from flask.views import MethodView

from factories import create_application, create_celery
from exceptions import APIError
import store

app = create_application()
celery = create_celery(app)


@app.errorhandler(APIError)
def handle_invalid_usage(error):
    response = jsonify(error.to_dict())
    response.status_code = error.status_code
    return response


class SymbolsAPI(MethodView):

    def get(self):
        return jsonify(store.get_symbols())

    def post(self):
        return store.add_symbols(request.form.getlist('symbol'))

    def delete(self):
        return store.remove_symbols(request.form.getlist('symbol'))


symbols_view = SymbolsAPI.as_view('symbols')
app.add_url_rule('/', view_func=symbols_view,
                 methods=['GET'])
app.add_url_rule('/symbols/', view_func=symbols_view,
                 methods=['GET', 'POST', 'DELETE'])


@app.route('/update/', methods=['GET'])
def update_all():
    return store.update_all()


@app.route('/query', methods=['POST'], defaults={'fields': 'ohlc'})
@app.route('/query/<fields>', methods=['POST'])
def query(fields):
    symbols = request.form.getlist('symbol')
    if not symbols:
        raise APIError("No symbols provided", 400)
    return store.query(symbols, fields)
