import os
import time

from factories import create_application, create_celery
from feeds import feed

celery = create_celery(create_application())


@celery.task
def update_data(symbols):
    for i, symbol in enumerate(symbols, 1):
        df = feed.load_single(symbol)
        df.to_pickle(
            os.path.join(celery.conf.DATA_DIR, symbol),
        )
        print("Successfully updated data for %s" % symbol)
        if i < len(symbols):
            time.sleep(12)
