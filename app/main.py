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


@app.route('/fetch/', methods=['GET'])
def fetch_all():
    return store.update_all()


@app.route('/single/<symbol>', methods=['GET', ])
def query_single(symbol):
    try:
        fields = request.args['fields']
    except KeyError:
        fields = 'ohlcavds'
    return store.query_single(symbol, fields)


@app.route('/batch/<field>', methods=['GET', ])
def query_batch(field):
    symbols = request.args.getlist('symbol')
    if not symbols:
        raise APIError("No symbols provided")
    return store.query_batch(symbols, field)


if __name__ == "__main__":
    app.run(host='0.0.0.0', debug=True)
