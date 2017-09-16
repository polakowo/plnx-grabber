import logging

import arrow
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
                        level=logging.DEBUG)

    grabber = plnxgrabber.Grabber(Poloniex(), get_db('TradeHistory'))

    start_ts = arrow.Arrow(2017, 9, 16, 19, 54, 19).timestamp
    start_id = 8638997
    end_ts = arrow.Arrow(2017, 9, 16, 19, 59, 14).timestamp
    end_id = 8639050

    grabber.drop_col('USDT_BTC')
    grabber.grab('USDT_BTC', start_ts=start_ts, end_ts=end_ts-60, end_id=end_id)


if __name__ == '__main__':
    main()
