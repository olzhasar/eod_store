import os
from flask import Flask
from celery import Celery

CELERY_BROKER_URL = os.environ.get('CELERY_BROKER_URL',
                                   'redis://localhost:6379')
CELERY_RESULTS_BACKEND = os.environ.get('CELERY_BROKER_URL',
                                        'redis://localhost:6379')
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, 'data/')


def create_application():
    app = Flask(__name__)
    app.config.update(
        DATA_DIR=DATA_DIR,
        SYMBOLS_FILE=os.path.join(DATA_DIR, 'symbols.csv')
    )
    return app


def create_celery(application):
    celery = Celery(
        application.import_name,
        broker=CELERY_BROKER_URL,
        backend=CELERY_RESULTS_BACKEND
    )
    celery.conf.update(application.config)

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
