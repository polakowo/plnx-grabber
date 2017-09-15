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
    logging.info("Started")

    grabber = plnxgrabber.Grabber(Poloniex(), get_db('TradeHistory'))
    # Collect the last minute history for a row of pairs
    grabber.row(['USDT_BTC', 'USDT_ETH', 'USDT_LTC'],
                start_ts=plnxgrabber.ts_ago(60),
                end_ts=plnxgrabber.ts_now(),
                drop=True)

    logging.info("Finished")


if __name__ == '__main__':
    main()
