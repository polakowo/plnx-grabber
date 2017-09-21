# Grabber of trade history from Poloniex exchange
# https://github.com/polakowo/plnx-grabber
#
#   Copyright (C) 2017  https://github.com/polakowo/plnx-grabber

#   This program is free software: you can redistribute it and/or modify
#   it under the terms of the GNU General Public License as published by
#   the Free Software Foundation, either version 3 of the License, or
#   (at your option) any later version.
#
#   This program is distributed in the hope that it will be useful,
#   but WITHOUT ANY WARRANTY; without even the implied warranty of
#   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#   GNU General Public License for more details.
#
#   You should have received a copy of the GNU General Public License
#   along with this program.  If not, see <http://www.gnu.org/licenses/>.

import logging
import math
import re
from enum import Enum
from time import sleep
from timeit import default_timer as timer

import arrow
import pandas as pd
import pymongo
from poloniex import Poloniex

# Logger
############################################################

logger = logging.getLogger(__name__)
# No logging by default
logger.addHandler(logging.NullHandler())


# Date & time
############################################################

def parse_date(date_str, fmt='YYYY-MM-DD HH:mm:ss'):
    # Parse dates coming from Poloniex
    return arrow.get(date_str, fmt)


def date_to_str(date, fmt='ddd DD/MM/YYYY HH:mm:ss ZZ'):
    # Format date for showing in console and logs
    return date.format(fmt)


def ts_to_str(ts, fmt='ddd DD/MM/YYYY HH:mm:ss ZZ'):
    return arrow.get(ts).format(fmt)


def now_ts():
    return arrow.utcnow().timestamp


def ago_ts(seconds):
    return arrow.utcnow().shift(seconds=-seconds).timestamp


def ts_from_date(date):
    return date.timestamp


def ts_to_date(ts):
    return arrow.get(ts)


def ts_delta(ts1, ts2):
    return abs(ts_to_date(ts1) - ts_to_date(ts2))


def td_format(td_object):
    seconds = int(abs(td_object).total_seconds())
    periods = [('year', 60 * 60 * 24 * 365),
               ('month', 60 * 60 * 24 * 30),
               ('day', 60 * 60 * 24),
               ('hour', 60 * 60),
               ('minute', 60),
               ('second', 1)]

    strings = []
    for period_name, period_seconds in periods:
        if seconds >= period_seconds:
            period_value, seconds = divmod(seconds, period_seconds)
            if period_value == 1:
                strings.append('%s %s' % (period_value, period_name))
            else:
                strings.append('%s %ss' % (period_value, period_name))

    return ' '.join(strings)


class TimePeriod(Enum):
    SECOND = 1
    MINUTE = 60 * SECOND
    HOUR = 60 * MINUTE
    DAY = 24 * HOUR
    WEEK = 7 * DAY
    MONTH = 30 * DAY
    YEAR = 12 * MONTH


# Dataframes
############################################################

def df_memory(df):
    return df.memory_usage(index=True, deep=True).sum()


def index_by_name(df, name):
    # Get values of an index level, in our case _id or ts
    return df.index.get_level_values(name)


def df_to_dict(df):
    # Convert df into shape suitable for export into MongoDB
    return df.reset_index().to_dict(orient='records')


def df_history_info(df):
    # Returns the most valuable information on history stored in df.
    # Get the order by comparing the first and last records.
    from_i = -1 * (df.index[0][0] > df.index[-1][0])
    to_i = -1 * (df.index[0][0] < df.index[-1][0])
    from_id, from_ts = df.index[from_i]
    to_id, to_ts = df.index[to_i]
    return {
        'from_ts': from_ts,
        'from_id': from_id,
        'to_ts': to_ts,
        'to_id': to_id,
        'delta': ts_delta(from_ts, to_ts),
        'count': len(df.index),
        'memory': df_memory(df)}


def verify_history_df(df):
    # Verifies the incremental nature of trade id across history
    t = timer()
    history_info = df_history_info(df)
    diff = history_info['count'] - (history_info['to_id'] - history_info['from_id'] + 1)
    if diff > 0:
        logger.warning("Dataframe - Found duplicates (%d) - %.2fs", diff, timer() - t)
    elif diff < 0:
        logger.warning("Dataframe - Found gaps (%d) - %.2fs", abs(diff), timer() - t)
    else:
        logger.debug("Dataframe - Verified - %.2fs", timer() - t)
    return diff == 0


def readable_bytes(num):
    """
    Read only first and latest records (cheap)
    :param num: number of bytes
    :return: readable string
    """
    for x in ['B', 'KB', 'MB', 'GB', 'TB']:
        if num < 1024.0:
            return '%3.1f %s' % (num, x)
        num /= 1024.0


def history_info_str(history_info):
    return "{ %s : %d, %s : %d, %s, %d rows, %s }" % (
        ts_to_str(history_info['from_ts']),
        history_info['from_id'],
        ts_to_str(history_info['to_ts']),
        history_info['to_id'],
        td_format(history_info['delta']),
        history_info['count'],
        readable_bytes(history_info['memory']))


# MongoDB Wrapper
############################################################

class TradeHistMongo(object):
    """
    Wrapper around pymongo for dealing with trade history information
    """

    def __init__(self, db):
        # Set running MongoDB instance
        self.db = db

    def db_info(self):
        # Aggregates basic info on current state of db
        cname_history_info = {cname: self.col_history_info(cname) for cname in self.db.collection_names()}
        logger.info("Database '{0}' - {1} collections - {2:,} documents - {3}"
                    .format(self.db.name,
                            len(cname_history_info),
                            sum(history_info['count'] for history_info in cname_history_info.values()),
                            readable_bytes(
                                sum(history_info['memory'] for history_info in cname_history_info.values()))))
        # Shows detailed descriptions of each collection
        for cname, history_info in cname_history_info.items():
            logger.info("%s - %s", cname, history_info_str(history_info))

    def clear_db(self):
        # Drop all collections
        for cname in self.col_list():
            self.drop_col(cname)

    # Collections

    def col_list(self):
        return self.db.collection_names()

    def create_col(self, cname):
        # Create new collection and index on timestamp field
        self.db.create_collection(cname)
        self.db[cname].create_index([('ts', pymongo.ASCENDING)], unique=False, background=True)

    def drop_col(self, cname):
        # Delete collection entirely
        self.db[cname].drop()
        logger.debug("%s - Dropped entirely", cname)

    def col_non_empty(self, cname):
        # Check whether collection exists and not empty
        return cname in self.db.collection_names() and self.col_count(cname) > 0

    def col_memory(self, cname):
        # Returns size of all documents + header + index size
        return self.db.command('collstats', cname)['size'] + 16 * 100 + self.db.command('collstats', cname)[
            'totalIndexSize']

    def col_count(self, cname):
        # Documents count in collection
        return self.db.command('collstats', cname)['count']

    def col_history_info(self, cname):
        # Returns the most important history information
        # (start and end points, their delta, num of rows and memory taken)
        from_dict = self.from_doc(cname)
        to_dict = self.to_doc(cname)
        return {
            'from_ts': from_dict['ts'],
            'from_id': from_dict['_id'],
            'to_ts': to_dict['ts'],
            'to_id': to_dict['_id'],
            'delta': ts_delta(from_dict['ts'], to_dict['ts']),
            'count': self.col_count(cname),
            'memory': self.col_memory(cname)}

    def verify_history_col(self, cname):
        # Verifies the incremental nature of trade id across history
        t = timer()
        history_info = self.col_history_info(cname)
        diff = history_info['count'] - (history_info['to_id'] - history_info['from_id'] + 1)
        if diff > 0:
            logger.warning("Collection - Found duplicates (%d) - %.2fs", diff, timer() - t)
        elif diff < 0:
            logger.warning("Collection - Found gaps (%d) - %.2fs", abs(diff), timer() - t)
        else:
            logger.debug("Collection - Verified - %.2fs", timer() - t)
        return diff == 0

    # Documents

    def from_doc(self, cname):
        # Return the document for the earliest point in history
        return next(self.db[cname].find().sort([['_id', 1]]).limit(1))

    def to_doc(self, cname):
        # Return the document for the most recent point in history
        return next(self.db[cname].find().sort([['_id', -1]]).limit(1))

    def insert_docs(self, cname, df):
        # Convert df into list of dicts and insert into collection (fast)
        t = timer()
        result = self.db[cname].insert_many(df_to_dict(df))
        logger.debug("%s - Collection - Inserted %d documents - %.2fs", cname, len(result.inserted_ids), timer() - t)

    def update_docs(self, cname, df):
        # Convert df into list of dicts and only insert records not present in the collection (slow)
        t = timer()
        n_modified = 0
        n_upserted = 0
        for record in df_to_dict(df):
            result = self.db[cname].update_one(
                {'_id': record['_id']},
                {'$setOnInsert': record},
                upsert=True)
            if result.modified_count is not None and result.modified_count > 0:
                n_modified += result.modified_count
            if result.upserted_id is not None:
                n_upserted += 1
        logger.debug("%s - Collection - Modified %d, upserted %d documents - %.2fs", cname, n_modified, n_upserted,
                     timer() - t)

    def delete_docs(self, cname, query={}):
        # Delete documents
        t = timer()
        result = self.db[cname].delete_many(query)
        logger.debug("%s - Collection - Deleted %d documents - %.2fs", cname, result.deleted_count, timer() - t)

    def find_docs(self, cname, query={}):
        # Return documents which match query
        return pd.DataFrame(list(self.db[cname].find(query))).set_index(['_id', 'ts'], drop=True)


# Grabber
############################################################

class Grabber(object):
    """
    Poloniex only returns max of 50,000 records at a time, meaning we have to coordinate download and
    save of many chunks of data. Moreover, there is no fixed amount of records per unit of time, which
    requires a synchronization of chunks by trade id.

    For example: If we would like to go one month back in time, Poloniex could have returned us only
    the most recent week. Because Polo returns only 50,000 of the latest records (not the oldest ones),
    we can synchronize chunks only by going backwards. Otherwise, if we decided to go forwards in time,
    we couldn't know which time interval to choose to fill all records in order to synchronize with
    previous chunk.
    """

    def __init__(self, db):
        # pymongo Wrapper
        self.mongo = TradeHistMongo(db)
        # Poloniex
        self.polo = Poloniex()

    def progress(self):
        """
        Shows how much history was grabbed so far in relation to overall available on Poloniex
        """
        cname_history_info = {cname: self.mongo.col_history_info(cname) for cname in self.mongo.col_list()}
        for pair, history_info in cname_history_info.items():
            # Get latest id
            df = self.get_chunk(pair, now_ts() - 15*60, now_ts())
            if df.empty:
                logger.info("%s - No information available", pair)
                continue
            max_id = df_history_info(df)['to_id']

            # Progress bar
            steps = 50
            below_rate = history_info['from_id'] / max_id
            taken_rate = (history_info['to_id'] - history_info['from_id']) / max_id
            above_rate = (max_id - history_info['to_id']) / max_id
            progress = '_' * math.floor(below_rate * steps) + \
                       'x' * (steps - math.floor(below_rate * steps) - math.floor(above_rate * steps)) + \
                       '_' * math.floor(above_rate * steps)

            logger.info("%s - 1 [ %s ] %d - %.1f/100.0%% - %s/%s",
                        pair,
                        progress,
                        history_info['to_id'],
                        taken_rate * 100,
                        readable_bytes(history_info['memory']),
                        readable_bytes(1 / taken_rate * history_info['memory']))

    def remote_info(self, pairs):
        """
        Detailed info on pairs listed on Poloniex
        """
        for pair in pairs:
            chart_data = Poloniex().returnChartData(pair, period=86400, start=1, end=now_ts())
            from_ts = chart_data[0]['date']
            to_ts = chart_data[-1]['date']

            df = self.get_chunk(pair, now_ts() - 300, now_ts())
            if df.empty:
                logger.info("%s - No information available")
                continue
            max_id = df_history_info(df)['to_id']

            logger.info("%s - %s - %s, %s, %d trades, est. %s",
                        pair,
                        ts_to_str(from_ts, fmt='ddd DD/MM/YYYY'),
                        ts_to_str(to_ts, fmt='ddd DD/MM/YYYY'),
                        td_format(ts_delta(from_ts, to_ts)),
                        max_id,
                        readable_bytes(round(df_memory(df) * max_id / len(df.index))))

    def db_info(self):
        """
        Wrapper for mongo.db_info
        """
        self.mongo.db_info()

    def ticker_pairs(self):
        """
        Returns all pairs from ticker
        """
        ticker = self.polo.returnTicker()
        pairs = set(map(lambda x: str(x).upper(), ticker.keys()))
        return pairs

    def get_chunk(self, pair, start, end):
        """
        Returns a chunk of trade history (max 50,000 of the most recent records) of a period of time

        :param pair: pair of symbols
        :param start: timestamp of start
        :param end: timestamp of end
        :return: df
        """
        try:
            history = self.polo.marketTradeHist(pair, start=start, end=end)
            history_df = pd.DataFrame(history)
            history_df = history_df.astype({
                'date': str,
                'amount': float,
                'globalTradeID': int,
                'rate': float,
                'total': float,
                'tradeID': int,
                'type': str})
            history_df['date'] = history_df['date'].apply(lambda date_str: ts_from_date(parse_date(date_str))).astype(
                int)
            history_df.rename(columns={'date': 'ts', 'tradeID': '_id', 'globalTradeID': 'globalid'}, inplace=True)
            history_df = history_df.set_index(['_id', 'ts'], drop=True)
            return history_df
        except Exception as e:
            return pd.DataFrame()

    def grab(self, pair, from_ts=None, from_id=None, to_ts=None, to_id=None):
        """
        Grabs trade history of a period of time for a pair of symbols.

        * Traverses history from the end date to the start date (backwards)
        * History is divided into chunks of max 50,000 records
        * Chunks are synced by id of their oldest records
        * Once received, each chunk is immediately put into MongoDB to free up RAM
        * Result includes passed dates - [from_ts, to_ts]
        * Result excludes passed ids - (from_id, to_id)
        * Ids have higher priority than dates
        
        The whole process looks like this:
        1) Start recording history chunk by chunk beginning from to_ts

                [ from_ts/from_id <- xxxxxxxxxxxxxxxxxxxxxxxxxx to_ts ]

            or if to_id is provided, find it first and only then start recording

                [ from_ts/from_id ___________ to_id <- <- <- <- to_ts ]

                [ from_ts/from_id <- xxxxxxxx to_id ___________ to_ts ]


        2) Each chunk is verified for consistency and inserted into MongoDB
        3) Proceed until start date or id are reached, or Poloniex returned nothing

                [ from_ts/from_id xxxxxxxxxxxxxxxxxxxxxxxxxxxxx to_ts ]
                                                |
                                                v
                                        collected history

            or if to_id is provided

                [ from_ts/from_id xxxxxxxxxxx to_id ___________ to_ts ]
                                         |
                                         v
                                 collected history

        4) Verify whole collection

        :param pair: pair of symbols
        :param from_ts: timestamp of start point (only as approximation, program aborts if found)
        :param from_id: id of start point (has higher priority than ts, program aborts if found)
        :param to_ts: timestamp of end point
        :param to_id: id of end point
        :return: None
        """
        if self.mongo.col_non_empty(pair):
            logger.debug("%s - Collection - %s", pair, history_info_str(self.mongo.col_history_info(pair)))
        else:
            logger.debug("%s - Collection - Empty", pair)

            # Create new collection only if none exists
            if pair not in self.mongo.col_list():
                self.mongo.create_col(pair)
        logger.debug("%s - Collection - Achieving { %s%s, %s%s, %s }",
                     pair,
                     ts_to_str(from_ts),
                     ' : %d' % from_id if from_id is not None else '',
                     ts_to_str(to_ts),
                     ' : %d' % to_id if to_id is not None else '',
                     td_format(ts_delta(from_ts, to_ts)))

        t = timer()

        # Init window params
        # ..................

        # Dates are required to build rolling windows and pass them to Poloniex
        # If start and/or end dates are empty, set the widest period possible
        if from_ts is None:
            from_ts = 0
        if to_ts is None:
            to_ts = now_ts()
        if to_ts <= from_ts:
            raise Exception("%s - Start date { %s } above end date { %s }" %
                            (pair, ts_to_str(from_ts), ts_to_str(to_ts)))
        if from_id is not None and to_id is not None:
            if to_id <= from_id:
                raise Exception("%s - Start id { %d } above end id { %d }" %
                                (pair, from_id, to_id))

        max_window_size = TimePeriod.MONTH.value
        window = {
            # Do not fetch more than needed, pick the size smaller or equal to max_window_size
            'from_ts': max(to_ts - max_window_size, from_ts),
            'to_ts': to_ts,
            # Gets filled after first chunk is fetched
            'anchor_id': None
        }
        # Record only starting from to_id, or immediately if none is provided
        recording = to_id is None
        # After we recorded data, verify consistency in database
        anything_recorded = False

        # Three possibilities to escape the loop:
        #   1) empty result
        #   2) reached the start date/id
        #   3) exception
        while True:
            t2 = timer()

            # Receive and process chunk of data
            # .................................

            logger.debug("%s - Poloniex - Querying { %s, %s, %s }",
                         pair,
                         ts_to_str(window['from_ts']),
                         ts_to_str(window['to_ts']),
                         td_format(ts_delta(window['from_ts'], window['to_ts'])))

            df = self.get_chunk(pair, window['from_ts'], window['to_ts'])
            if df.empty:
                if anything_recorded or window['from_ts'] == from_ts:
                    # If we finished (either by reaching start or receiving no records) -> terminate
                    logger.debug("%s - Poloniex - Nothing returned - aborting", pair)
                    break
                else:
                    # If Poloniex temporary suspended trading for a pair -> look for older records
                    logger.debug("%s - Poloniex - Nothing returned - continuing", pair)
                    window['to_ts'] = window['from_ts']
                    window['from_ts'] = max(window['from_ts'] - max_window_size, from_ts)
                    continue

            # If chunk contains end id (newest bound) -> start recording
            # .........................................................

            if not recording:
                # End id found
                if to_id in index_by_name(df, '_id'):
                    logger.debug("%s - Poloniex - End id { %d } found", pair, to_id)
                    # Start recording
                    recording = True

                    df = df[index_by_name(df, '_id') < to_id]
                    if df.empty:
                        logger.debug("%s - Poloniex - Nothing returned - aborting", pair)
                        break
                else:
                    history_info = df_history_info(df)
                    logger.debug("%s - Poloniex - End id { %d } not found in { %s : %d, %s : %d }",
                                 pair,
                                 to_id,
                                 ts_to_str(history_info['from_ts']),
                                 history_info['from_id'],
                                 ts_to_str(history_info['to_ts']),
                                 history_info['to_id'])

                    # If start reached -> terminate
                    if from_id is not None:
                        if any(index_by_name(df, '_id') <= from_id):
                            logger.debug("%s - Poloniex - Start id { %d } reached - aborting", pair, from_id)
                            break
                    if any(index_by_name(df, 'ts') <= from_ts):
                        logger.debug("%s - Poloniex - Start date { %s } reached - aborting", pair, ts_to_str(from_ts))
                        break

                    history_info = df_history_info(df)
                    window['from_ts'] = max(history_info['from_ts'] - max_window_size, from_ts)
                    window['to_ts'] = history_info['from_ts']
                    continue

            if recording:

                # Synchronize with previous chunk by intersection of their ids
                # ............................................................

                if window['anchor_id'] is not None:
                    # To merge two dataframes, there must be an intersection of ids (anchor)
                    if any(index_by_name(df, '_id') >= window['anchor_id']):
                        df = df[index_by_name(df, '_id') < window['anchor_id']]
                        if df.empty:
                            logger.debug("%s - Poloniex - Nothing returned - aborting", pair)
                            break
                    else:
                        logger.debug("%s - Poloniex - Anchor id { %d } is missing - aborting", pair,
                                     window['anchor_id'])
                        break

                # If chunk contains start id or date (oldest record) -> finish recording
                # ....................................................................

                if from_id is not None:
                    if any(index_by_name(df, '_id') <= from_id):
                        df = df[index_by_name(df, '_id') > from_id]
                        if df.empty:
                            logger.debug("%s - Poloniex - Nothing returned - aborting", pair)
                        else:
                            logger.debug("%s - Poloniex - Returned %s - %.2fs",
                                         pair, history_info_str(df_history_info(df)), timer() - t2)
                            logger.debug("%s - Poloniex - Start id { %d } reached - aborting", pair, from_id)
                            if verify_history_df(df):
                                self.mongo.insert_docs(pair, df)
                                anything_recorded = True
                        break  # escape anyway
                # or at least the approx. date
                elif any(index_by_name(df, 'ts') <= from_ts):
                    df = df[index_by_name(df, 'ts') >= from_ts]
                    if df.empty:
                        logger.debug("%s - Poloniex - Nothing returned - aborting", pair)
                    else:
                        logger.debug("%s - Poloniex - Returned %s - %.2fs",
                                     pair, history_info_str(df_history_info(df)), timer() - t2)
                        logger.debug("%s - Poloniex - Start date { %s } reached - aborting", pair, ts_to_str(from_ts))
                        if verify_history_df(df):
                            self.mongo.insert_docs(pair, df)
                            anything_recorded = True
                    break

                # Record data
                # ...........

                # Drop rows with NaNs
                df.dropna(inplace=True)
                if df.empty:
                    logger.debug("%s - Poloniex - Nothing returned - aborting", pair)
                    break
                # Drop duplicates
                df.drop_duplicates(inplace=True)
                if df.empty:
                    logger.debug("%s - Poloniex - Nothing returned - aborting", pair)
                    break

                # If none of the start points reached, continue with execution using new window
                logger.debug("%s - Poloniex - Returned %s - %.2fs",
                             pair, history_info_str(df_history_info(df)), timer() - t2)
                # Break on last stored df if the newest chunk is broken
                if not verify_history_df(df):
                    break
                self.mongo.insert_docs(pair, df)
                anything_recorded = True

                # Continue with next chunk
                # ........................

                history_info = df_history_info(df)
                window['from_ts'] = max(history_info['from_ts'] - max_window_size, from_ts)
                window['to_ts'] = history_info['from_ts']
                window['anchor_id'] = history_info['from_id']

        # Verify collection after recordings
        # ..................................

        if anything_recorded:
            if self.mongo.verify_history_col(pair):
                logger.debug("%s - Collection - %s - %.2fs", pair, history_info_str(self.mongo.col_history_info(pair)),
                             timer() - t)
            else:
                raise Exception("%s - Consistency broken - fix required" % pair)
        else:
            logger.debug("%s - Nothing returned - %.2fs", pair, timer() - t)

    def one(self, pair, from_ts=None, to_ts=None, drop=False):

        """
        Grabs data for a pair based on passed params as well as history stored in the underlying collection

        Possible values of from_ts and to_ts:
        * 'oldest' means the from_ts of the collection
        * 'newest' means the to_ts of the collection

        :param pair: pair of symbols
        :param from_ts: timestamp of the start point or command from ['oldest', 'newest']
        :param to_ts: timestamp of the end point or command from ['oldest', 'newest']
        :param drop: delete underlying collection before insert
        :return: None
        """
        t = timer()
        logger.info("%s - ...", pair)

        # Fill timestamps of collection's bounds
        if self.mongo.col_non_empty(pair):
            history_info = self.mongo.col_history_info(pair)

            if isinstance(from_ts, str):
                if from_ts == 'oldest':
                    from_ts = history_info['from_ts']
                elif from_ts == 'newest':
                    from_ts = history_info['to_ts']
                else:
                    raise Exception("Unknown command '%s'" % from_ts)
            if isinstance(to_ts, str):
                if to_ts == 'oldest':
                    to_ts = history_info['from_ts']
                elif to_ts == 'newest':
                    to_ts = history_info['to_ts']
                else:
                    raise Exception("Unknown command '%s'" % to_ts)

            # Overwrite means drop completely
            if drop:
                self.mongo.drop_col(pair)

        # If nothing is passed, fetch the widest tail and/or head possible
        if from_ts is None:
            from_ts = 0
        if to_ts is None:
            to_ts = now_ts()

        if self.mongo.col_non_empty(pair):
            history_info = self.mongo.col_history_info(pair)

            # Period must be non-zero
            if from_ts >= to_ts:
                raise Exception("%s - Start date { %s } above end date { %s }" %
                                (pair, ts_to_str(from_ts), ts_to_str(to_ts)))

            if from_ts < history_info['from_ts']:
                logger.debug("%s - Grabbing tail", pair)
                # Collect history up to the oldest record
                self.grab(pair,
                          from_ts=from_ts,
                          to_ts=history_info['from_ts'],
                          to_id=history_info['from_id'])

            if to_ts > history_info['to_ts']:
                logger.debug("%s - Grabbing head", pair)
                # Collect history from the newest record
                self.grab(pair,
                          from_ts=history_info['to_ts'],
                          to_ts=to_ts,
                          from_id=history_info['to_id'])
        else:
            # There is no newest or oldest bounds of empty collection
            if isinstance(from_ts, str) or isinstance(to_ts, str):
                raise Exception("%s - Collection empty - cannot auto-fill timestamps" % pair)

            logger.debug("%s - Grabbing full", pair)
            self.grab(pair,
                      from_ts=from_ts,
                      to_ts=to_ts)

        logger.info("%s - Finished - %.2fs", pair, timer() - t)

    def row(self, pairs, from_ts=None, to_ts=None, drop=False):
        """
        Grabs data for each pair in a row

        :param pairs: list of pairs or string command from ['db', 'ticker']
        :param from_ts: timestamp of the start point or command from ['oldest', 'newest']
        :param to_ts: timestamp of the end point or command from ['oldest', 'newest']
        :param drop: delete underlying collection before insert
        :return: None
        """
        if isinstance(pairs, str):
            # All pairs in db
            if pairs == 'db':
                pairs = self.mongo.col_list()
            # All pairs in ticker
            elif pairs == 'ticker':
                pairs = self.ticker_pairs()
            else:
                regex = re.compile(pairs)
                pairs = list(filter(regex.search, self.ticker_pairs()))
        if len(pairs) == 0:
            raise Exception("List of pairs must be non-empty")
        for pair in pairs:
            t = timer()
            self.one(pair, from_ts=from_ts, to_ts=to_ts, drop=drop)

    def ring(self, pairs, every=None):
        """
        Grabs the most recent data for a row of pairs on repeat

        Requires all pairs to be persistent in the database

        :param pairs: list of pairs or 'db' command
        :param every: pause between iterations
        :return: None
        """
        if isinstance(pairs, str):
            # All pairs in db
            if pairs == 'db':
                pairs = self.mongo.col_list()
            else:
                regex = re.compile(pairs)
                pairs = list(filter(regex.search, self.ticker_pairs()))
        if len(pairs) == 0:
            raise Exception("List of pairs must be non-empty")
        while True:
            # Collect head every time interval
            self.row(pairs, to_ts=now_ts())
            if every is not None:
                sleep(every)
