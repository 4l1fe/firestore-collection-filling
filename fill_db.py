import logging
import shelve
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, wait
from collections import defaultdict
from itertools import cycle
from copy import deepcopy

import pymysql
from google.cloud import firestore


COPIES_N = 100
THREADS = 15
PROCESSES = 60
CACHE_FILE = 'cache'
KEY_FILE = 'KeysStorage.json'
HOST = ''
USER = ''
PASSWORD = ''
DB = ''
CHARSET = ''
QUERY = """"""


def disk_cache(func):
    key = func.__name__

    def decorator(*args, **kwargs):
        with shelve.open(CACHE_FILE) as db:
            data = db.get(key)

        if not data:
            data = func(*args, **kwargs)
            with shelve.open(CACHE_FILE) as db:
                db[key] = data
        return data

    return decorator


@disk_cache
def get_source_data():
    logging.info('get source data')
    with pymysql.connect(host=HOST, user=USER, password=PASSWORD, db=DB, charset=CHARSET) as cursor:
        cursor.execute(QUERY)
        data = cursor.fetchall()
    logging.info('end get source data')
    return data


@disk_cache
def group_documents(data):
    documents = defaultdict(lambda: defaultdict(list))
    for row in data:

        idx = (row[0],row[1],row[2])
        documents[idx]['_v'] = 1
        documents[idx]['k'] = row[0]
        documents[idx]['c'] = row[1]
        documents[idx]['d'] = row[2]
        documents[idx]['created'] = datetime.utcnow()
        documents[idx]['p'].append(row[5])

        if len(documents.keys()) >= THREADS:
            break

    return dict(documents)


def add(document):
    logging.debug(f'start add')
    db = firestore.Client.from_service_account_json(KEY_FILE)
    for i in range(COPIES_N):
        db.collection('cln').add(document)


def concurrent_add(documents):
    logging.info(f'start concurrent add')

    futures = []
    with ThreadPoolExecutor(max_workers=THREADS) as pool:
        _documents = cycle(documents.values())
        step = 1
        while step != THREADS:
            d = next(_documents)
            futures.append(pool.submit(add, d))
            step += 1
        done, not_done = wait(futures)

    errors = 0
    error_kinds = set()
    for t in done:
        error = t.exception()
        errors += bool(error)
        error_kinds.add(error)

    if None in error_kinds:
        error_kinds.remove(None)
    logging.info(f'errors : {errors}')
    logging.info(f'error kind : {error_kinds}')
    logging.info(f'not done: {len(not_done)}')


def _daily_docs(days, documents):
    _documents = deepcopy(documents)

    for _, doc in _documents.items():
        doc['created'] = doc['created'] + timedelta(days=days)

    return _documents


def main():
    logging.info('start main')
    data = get_source_data()
    documents = group_documents(data)

    futures = []
    with ProcessPoolExecutor(max_workers=PROCESSES) as pool:
        for days in range(PROCESSES):
            futures.append( pool.submit(concurrent_add, _daily_docs(days, documents)) )

        done, not_done = wait(futures)
        logging.info(f'{len(done)}')
        logging.info(f'{done.pop().result()}')

    logging.info('end main')


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='[%(asctime)s %(process)d %(thread)d %(levelname)s] %(message)s')
    main()
