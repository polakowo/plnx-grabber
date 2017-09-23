from arrow import Arrow
from pymongo import MongoClient

import mongots
import arrow


def get_db(name):
    client = MongoClient('localhost:27017')
    db = client[name]
    return db


def main():
    db = get_db('TradeHistory')
    mongo = mongots.MongoTS(db)

    from_ts = arrow.Arrow(2017, 9, 5, 0, 0, 0).timestamp
    to_ts = arrow.Arrow(2017, 9, 5, 0, 5, 0).timestamp
    print(mongo.series_candlesticks('USDT_ETH', from_ts, to_ts))


if __name__ == '__main__':
    main()
