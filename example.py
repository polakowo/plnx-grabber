import logging
from datetime import datetime

import pytz
from pymongo import MongoClient

import plnxgrabber


def main():
    logging.basicConfig(format='%(asctime)s - %(name)s - %(funcName)s() - %(levelname)s - %(message)s',
                        datefmt='%d/%m/%Y %H:%M:%S',
                        level=logging.INFO)

    # logging.basicConfig(filename='plnxgrabber.log',
    #                     filemode='w',
    #                     format='%(asctime)s - %(name)s - %(funcName)s() - %(levelname)s - %(message)s',
    #                     datefmt='%d/%m/%Y %H:%M:%S',
    #                     level=logging.DEBUG)

    client = MongoClient('localhost:27017')
    db = client['TradeHistory']
    mongo_ts = plnxgrabber.MongoTS(db)
    grabber = plnxgrabber.Grabber(mongo_ts)

    try:
        from_dt = datetime(2017, 9, 5, 0, 0, 0, tzinfo=pytz.utc)
        to_dt = datetime(2017, 9, 5, 0, 30, 0, tzinfo=pytz.utc)
        grabber.one('USDT_LTC', from_dt=from_dt, to_dt=to_dt, drop=True)
    except Exception as e:
        logging.exception(e)

    grabber.db_info()


if __name__ == '__main__':
    main()
