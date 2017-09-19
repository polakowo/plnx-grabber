import logging

import arrow
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

    db = get_db('TradeHistory')
    grabber = plnxgrabber.Grabber(db)

    # Fetch 5 minutes
    start_ts = arrow.Arrow(2017, 9, 1, 12, 0, 0).timestamp
    # start_id = 7821708
    end_ts = arrow.Arrow(2017, 9, 1, 18, 0, 0).timestamp
    # end_id = 7821761

    logging.info("Row - 4 pairs - from 1/9/2017 12:00:00 to 1/9/2017 18:00:00")
    grabber.row(['USDT_BTC', 'USDT_ETH', 'USDT_LTC', 'USDT_BCH'], start_ts=start_ts, end_ts=end_ts, overwrite=True)


if __name__ == '__main__':
    main()
