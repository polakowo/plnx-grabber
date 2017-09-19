import logging

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

    grabber.row('(ETH_+)', from_ts=plnxgrabber.ago_ts(60), to_ts=plnxgrabber.now_ts(), drop=True)


if __name__ == '__main__':
    main()
