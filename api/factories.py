import os
from flask import Flask, jsonify
from celery import Celery

from .exceptions import APIError

CELERY_BROKER_URL = os.environ.get('CELERY_BROKER_URL',
                                   'redis://localhost:6379')
CELERY_RESULTS_BACKEND = os.environ.get('CELERY_BROKER_URL',
                                        'redis://localhost:6379')
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(os.path.dirname(BASE_DIR), 'data/')


def create_application(test_config=None):
    app = Flask(__name__)
    app.config.from_mapping(
        DATA_DIR=DATA_DIR,
        SYMBOLS_FILE=os.path.join(BASE_DIR, 'symbols.json'),
    )

    if test_config is None:
        app.config.from_pyfile('config.py', silent=True)
    else:
        app.config.update(test_config)

    @app.errorhandler(APIError)
    def handle_api_error(error):
        response = jsonify(error.to_dict())
        response.status_code = error.status_code
        return response

    from . import store
    app.register_blueprint(store.bp)

    return app


def create_celery(application, **kwargs):
    celery = Celery(
        application.import_name,
        broker=CELERY_BROKER_URL,
        backend=CELERY_RESULTS_BACKEND
    )
    celery.conf.update(application.config, **kwargs)

    class ContextTask(celery.Task):
        def __call__(self, *args, **kwargs):
            with application.app_context():
                return self.run(*args, **kwargs)

    celery.Task = ContextTask

    """
    celery.conf.beat_schedule = {
        'print-every-30-seconds': {
            'task': 'store.print_something',
            'schedule': 30.0,
            'args': ()
        },
    }
    celery.conf.timezone = 'UTC'
    """

    return celery
