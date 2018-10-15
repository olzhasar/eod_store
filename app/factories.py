import os
from flask import Flask
from celery import Celery

CELERY_BROKER_URL = os.environ.get('CELERY_BROKER_URL',
                                   'redis://localhost:6379')
CELERY_RESULTS_BACKEND = os.environ.get('CELERY_BROKER_URL',
                                        'redis://localhost:6379')


def create_application():
    app = Flask(__name__)
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
    return celery
