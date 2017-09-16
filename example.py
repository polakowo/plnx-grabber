import logging

from poloniex import Poloniex
from pymongo import MongoClient

import plnxgrabber


def get_db(name):
    client = MongoClient('localhost:27017')
    db = client[name]
    return db


def main():
    logging.basicConfig(format='%(asctime)s - %(name)s - %(funcName)s() - %(levelname)s - %(message)s',
                        datefmt='%d/%m/%Y %H:%M:%S',
                        level=logging.INFO)

    grabber = plnxgrabber.Grabber(Poloniex(), get_db('TradeHistory'))

    # Grab last day of history for a row of pairs and keep updating every 60 sec
    grabber.ring(["USDT_BTC", "USDT_ETH"], start_ts=plnxgrabber.ts_ago(60 * 60 * 24), drop=True, every=60)


if __name__ == '__main__':
    main()
