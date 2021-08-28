import asyncio
from os import environ
import requests

from src.core.http.QueueItem import QueueItem
from src.core import logger

session = requests.Session()

queueman_fetch_url = environ.get('FETCH_URL', 'http://127.0.0.1:3005/item')

def fetch_item():
    try:
        resp = session.get(queueman_fetch_url, timeout=900)
        resp.raise_for_status()
        return QueueItem(**(resp.json()))
    except Exception as ex:
        logger.warning('Error while fetching work item')
        logger.warning(ex)


def __main__():
    while True:
        logger.info("Fetching work item")
        item = fetch_item()
        if not item is None:
            try:
                item.process()
            except Exception as ex:
                logger.warning('Failed to process item')
                logger.warning(ex)


if __name__ == '__main__':
    __main__()
