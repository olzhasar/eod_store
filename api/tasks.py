import os
import time
from factories import make_celery
from feeds import feed

celery = make_celery()


@celery.task('tasks.update_all')
def update_data(symbols):
    for i, symbol in enumerate(symbols, 1):
        df = feed.load_single(symbol)
        df.to_hdf(
            os.path.join(celery.conf.DATA_DIR, symbol),
            key=symbol,
            mode='w'
        )
        print("Successfully updated data for %s" % symbol)
        if i < len(symbols):
            time.sleep(20)
