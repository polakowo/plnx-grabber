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
from collections import defaultdict
from datetime import timedelta
from enum import Enum
from time import sleep
from timeit import default_timer as timer

import arrow
import pandas as pd

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
    MINUTE = 60*SECOND
    HOUR = 60*MINUTE
    DAY = 24*HOUR
    WEEK = 7*DAY
    MONTH = 30*DAY
    YEAR = 12*MONTH


# Dataframes
############################################################

def df_memory(df):
    return df.memory_usage(index=True).sum()


def index_by_name(df, name):
    # Get values of an index level, in our case _id or ts
    return df.index.get_level_values(name)


def df_to_dict(df):
    # Convert df into shape suitable for export into MongoDB
    return df.reset_index().to_dict(orient='records')


def df_history_info(df):
    # Returns the most valuable information on history stored in df.
    # Get the order by comparing the first and last records.
    start_i = -1 * (df.index[0][0] > df.index[-1][0])
    end_i = -1 * (df.index[0][0] < df.index[-1][0])
    start_id, start_ts = df.index[start_i]
    end_id, end_ts = df.index[end_i]
    return {
        'start_ts': start_ts,
        'start_id': start_id,
        'end_ts': end_ts,
        'end_id': end_id,
        'delta': ts_delta(start_ts, end_ts),
        'count': len(df.index),
        'memory': df_memory(df)}


def verify_history_df(df):
    # Verifies the incremental nature of trade id across history
    t = timer()
    history_info = df_history_info(df)
    diff = history_info['count'] - (history_info['end_id'] - history_info['start_id'] + 1)
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
        ts_to_str(history_info['start_ts']),
        history_info['start_id'],
        ts_to_str(history_info['end_ts']),
        history_info['end_id'],
        td_format(history_info['delta']),
        history_info['count'],
        readable_bytes(history_info['memory']))


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

    def __init__(self, polo, db):
        # Set Poloniex API wrapper
        self.polo = polo
        # Set running MongoDB instance
        self.db = db

    # Database
    ############################################################

    def db_short_info(self):
        # Aggregates basic info on current state of db
        logger.info("Database '{0}' - {1} collections - {2:,} documents"
                    .format(self.db.name, len(self.db.collection_names()),
                            sum(self.db[cname].count() for cname in self.db.collection_names())))

    def db_long_info(self):
        # Shows detailed descriptions of each collection
        logger.info(''.join(["\n{0} - {1}".format(cname, history_info_str(self.col_history_info(cname))) for cname in
                             self.db.collection_names()]))

    # Collections

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
        start_dict = self.start_doc(cname)
        end_dict = self.end_doc(cname)
        return {
            'start_ts': start_dict['ts'],
            'start_id': start_dict['_id'],
            'end_ts': end_dict['ts'],
            'end_id': end_dict['_id'],
            'delta': ts_delta(start_dict['ts'], end_dict['ts']),
            'count': self.col_count(cname),
            'memory': self.col_memory(cname)}


    def verify_history_col(self, cname):
        # Verifies the incremental nature of trade id across history
        t = timer()
        history_info = self.col_history_info(cname)
        diff = history_info['count'] - (history_info['end_id'] - history_info['start_id'] + 1)
        if diff > 0:
            logger.warning("Collection - Found duplicates (%d) - %.2fs", diff, timer() - t)
        elif diff < 0:
            logger.warning("Collection - Found gaps (%d) - %.2fs", abs(diff), timer() - t)
        else:
            logger.debug("Collection - Verified - %.2fs", timer() - t)
        return diff == 0

    # Documents

    def start_doc(self, cname):
        # Return the document for the earliest point in history
        return next(self.db[cname].find().sort([['_id', 1]]).limit(1))

    def end_doc(self, cname):
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

    def delete_docs(self, cname, query=dict):
        # Delete documents
        t = timer()
        result = self.db[cname].delete_many(query)
        logger.debug("%s - Collection - Deleted %d documents - %.2fs", cname, result.deleted_count, timer() - t)

    def find_docs(self, cname, query=dict):
        # Return documents which match query
        return self.db[cname].find(query)

    # Poloniex
    ############################################################

    def return_symbols(self):
        ticker = self.polo.returnTicker()
        pairs = set(map(lambda x: str(x).upper(), ticker.keys()))
        pairs_grouped = defaultdict(list)
        for pair in pairs:
            pairs_grouped[pair.split('_')[0]].append(pair.split('_')[1])
        return pairs_grouped

    def return_trade_history(self, pair, start, end):
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

    # Grabber
    ############################################################

    def grab(self, pair, start_ts=None, start_id=None, end_ts=None, end_id=None):
        """
        Grabs trade history of a period of time for a pair of symbols.

        * Traverses history from the end date to the start date (backwards)
        * History is divided into chunks of max 50,000 records
        * Chunks are synced by id of their oldest records
        * Once received, each chunk is immediately put into MongoDB to free up RAM
        * Result includes passed dates - [start_ts, end_ts]
        * Result excludes passed ids - (start_id, end_id)
        * Ids have higher priority than dates
        
        The whole process looks like this:
        1) Start recording history chunk by chunk beginning from end_ts

                [ start_ts/start_id <- xxxxxxxxxxxxxxxxxxxxxxxxxxx end_ts ]

            or if end_id is provided, find it first and only then start recording

                [ start_ts/start_id ___________ end_id <- <- <- <- end_ts ]

                [ start_ts/start_id <- xxxxxxxx end_id ___________ end_ts ]


        2) Each chunk is verified for consistency and inserted into MongoDB
        3) Proceed until start date or id are reached, or Poloniex returned nothing

                [ start_ts/start_id xxxxxxxxxxxxxxxxxxxxxxxxxxxxxx end_ts ]
                                                   |
                                                   v
                                           collected history

            or if end_id is provided

                [ start_ts/start_id xxxxxxxxxxx end_id ___________ end_ts ]
                                         |
                                         v
                                 collected history

        4) Verify whole collection

        :param pair: pair of symbols
        :param start_ts: timestamp of start point (only as approximation, program aborts if found)
        :param start_id: id of start point (has higher priority than ts, program aborts if found)
        :param end_ts: timestamp of end point
        :param end_id: id of end point
        :return: None
        """
        if self.col_non_empty(pair):
            logger.debug("%s - Collection - %s", pair, history_info_str(self.col_history_info(pair)))
        else:
            logger.debug("%s - Collection - Empty", pair)
        logger.debug("%s - Collection - Achieving { %s%s, %s%s, %s }",
                     pair,
                     ts_to_str(start_ts),
                     ' : %d' % start_id if start_id is not None else '',
                     ts_to_str(end_ts),
                     ' : %d' % end_id if end_id is not None else '',
                     td_format(ts_delta(start_ts, end_ts)))

        t = timer()

        # Init window params
        # ..................

        # Dates are required to build rolling windows and pass them to Poloniex
        # If start and/or end dates are empty, set the widest period possible
        if start_ts is None:
            start_ts = 0
        if end_ts is None:
            end_ts = now_ts()
        if end_ts <= start_ts:
            raise Exception("%s - Start date { %s } above end date { %s }"%
                            (pair, ts_to_str(start_ts), ts_to_str(end_ts)))
        if start_id is not None and end_id is not None:
            if end_id <= start_id:
                raise Exception("%s - Start date { %d } above end date { %d }"%
                                (pair, start_id, end_id))

        max_window_size = TimePeriod.MONTH.value
        window = {
            # Do not fetch more than needed, pick the size smaller or equal to max_window_size
            'start_ts': max(end_ts - max_window_size, start_ts),
            'end_ts': end_ts,
            # Gets filled after first chunk is fetched
            'anchor_id': None
        }
        # Record only starting from end_id, or immediately if none is provided
        recording = end_id is None
        # After we recorded data, verify consistency in database
        anything_recorded = False

        # Three possibilities to escape the loop:
        #   1) empty result
        #   2) reached the start date/id
        #   3) exception
        while (True):
            t2 = timer()

            # Receive and process chunk of data
            # .................................

            logger.debug("%s - Poloniex - Querying { %s, %s, %s }",
                         pair,
                         ts_to_str(window['start_ts']),
                         ts_to_str(window['end_ts']),
                         td_format(ts_delta(window['start_ts'], window['end_ts'])))

            df = self.return_trade_history(pair, window['start_ts'], window['end_ts'])
            if df.empty:
                logger.debug("%s - Poloniex - Nothing returned - aborting", pair)
                break

            # If chunk contains end id (upper bound) -> start recording
            # .........................................................

            if not recording:
                # End id found
                if end_id in index_by_name(df, '_id'):
                    logger.debug("%s - Poloniex - End id { %d } found", pair, end_id)
                    # Start recording
                    recording = True

                    df = df[index_by_name(df, '_id') < end_id]
                    if df.empty:
                        logger.debug("%s - Poloniex - Nothing returned - aborting", pair)
                        break
                else:
                    history_info = df_history_info(df)
                    logger.debug("%s - Poloniex - End id { %d } not found in { %s : %d, %s : %d }",
                                 pair,
                                 end_id,
                                 ts_to_str(history_info['start_ts']),
                                 history_info['start_id'],
                                 ts_to_str(history_info['end_ts']),
                                 history_info['end_id'])

                    # If start reached -> terminate
                    if start_id is not None:
                        if any(index_by_name(df, '_id') <= start_id):
                            logger.debug("%s - Poloniex - Start id { %d } reached - aborting", pair, start_id)
                            break
                    if any(index_by_name(df, 'ts') <= start_ts):
                        logger.debug("%s - Poloniex - Start date { %s } reached - aborting", pair, ts_to_str(start_ts))
                        break

                    history_info = df_history_info(df)
                    window['start_ts'] = max(history_info['start_ts'] - max_window_size, start_ts)
                    window['end_ts'] = history_info['start_ts']
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

                # If chunk contains start id or date (lower bound) -> finish recording
                # ....................................................................

                if start_id is not None:
                    if any(index_by_name(df, '_id') <= start_id):
                        df = df[index_by_name(df, '_id') > start_id]
                        if df.empty:
                            logger.debug("%s - Poloniex - Nothing returned - aborting", pair)
                        else:
                            logger.debug("%s - Poloniex - Returned %s - %.2fs",
                                         pair, history_info_str(df_history_info(df)), timer() - t2)
                            logger.debug("%s - Poloniex - Start id { %d } reached - aborting", pair, start_id)
                            if verify_history_df(df):
                                self.insert_docs(pair, df)
                                anything_recorded = True
                        break  # escape anyway
                # or at least the approx. date
                elif any(index_by_name(df, 'ts') <= start_ts):
                    df = df[index_by_name(df, 'ts') >= start_ts]
                    if df.empty:
                        logger.debug("%s - Poloniex - Nothing returned - aborting", pair)
                    else:
                        logger.debug("%s - Poloniex - Returned %s - %.2fs",
                                     pair, history_info_str(df_history_info(df)), timer() - t2)
                        logger.debug("%s - Poloniex - Start date { %s } reached - aborting", pair, ts_to_str(start_ts))
                        if verify_history_df(df):
                            self.insert_docs(pair, df)
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
                self.insert_docs(pair, df)
                anything_recorded = True

                # Continue with next chunk
                # ........................

                history_info = df_history_info(df)
                window['start_ts'] = max(history_info['start_ts'] - max_window_size, start_ts)
                window['end_ts'] = history_info['start_ts']
                window['anchor_id'] = history_info['start_id']


        # Verify collection after recordings
        # ..................................

        if anything_recorded:
            if self.verify_history_col(pair):
                logger.debug("%s - Collection - %s - %.2fs", pair, history_info_str(self.col_history_info(pair)),
                             timer() - t)
            else:
                raise Exception("%s - Consistency broken - fix required" % pair)
        else:
            logger.debug("%s - Nothing returned - %.2fs", pair, timer() - t)

    def one(self, pair, start_ts=None, end_ts=None, overwrite=False):
        """
        Grabs data for a pair based on passed params as well as history stored in the underlying collection

        There are 5 supported configurations:

            History not provided or overwritten:

            1) Full dataset is fetched
                [ start_ts __________________FULL___________________ end_ts ]

            History provided:

            2) start_ts, end_ts: Both, tail and head datasets are fetched
                [ start_ts _____TAIL_____ xxxxxxxxxxx _____HEAD_____ end_ts ]

            3) start_ts=None, end_ts: Only head dataset is fetched
                [ xxxxxxxxxxx _________________HEAD_________________ end_ts ]

            4) start_ts, end_ts=None: Only tail dataset is fetched
                [ start_ts ________________TAIL________________ xxxxxxxxxxx ]

            5) start_ts=None, end_ts=None: History is completely filled from both sides
                [ start_ts _____TAIL_____ xxxxxxxxxxx _____HEAD_____ end_ts ]

            To avoid gaps:

            Start point is forced to the left and then head is fetched
                [ xxxxxxxxxxx ____________<- start_ts _____HEAD_____ end_ts ]

            End point is forced to the right and then tail is fetched
                [ start_ts _____TAIL_____ end_ts ->____________ xxxxxxxxxxx ]


        :param pair: pair of symbols
        :param start_ts: timestamp of the start point
        :param end_ts: timestamp of the end point
        :param overwrite: delete underlying collection before insert
        :return: None
        """
        if self.col_non_empty(pair) and overwrite:
            self.drop_col(pair)

        if self.col_non_empty(pair):
            # If nothing is passed, fetch the widest tail and head possible
            if start_ts is None and end_ts is None:
                start_ts = 0
                end_ts = now_ts()

            history_info = self.col_history_info(pair)
            # If start timestamp is given, extend the collection backwards in time
            if start_ts is not None:
                if start_ts < history_info['start_ts']:
                    logger.debug("%s - TAIL", pair)
                    self.grab(pair,
                              start_ts=start_ts,
                              end_ts=history_info['start_ts'],
                              end_id=history_info['start_id'])
            # If end timestamp is given, extend the collection forwards in time
            if end_ts is not None:
                if end_ts > history_info['end_ts']:
                    logger.debug("%s - HEAD", pair)
                    self.grab(pair,
                              start_ts=history_info['end_ts'],
                              end_ts=end_ts,
                              start_id=history_info['end_id'])
        else:
            # Just pass arguments to grab()
            logger.debug("%s - FULL", pair)
            self.grab(pair,
                      start_ts=start_ts,
                      end_ts=end_ts)

    def row(self, pairs, start_ts=None, end_ts=None, overwrite=False):
        """
        Grabs data for each pair in a row

        :param pairs: list of pairs
        :param start_ts: timestamp of the start point
        :param end_ts: timestamp of the end point
        :param overwrite: delete underlying collection before insert
        :return: None
        """
        for i, pair in enumerate(pairs):
            t = timer()
            logger.info("%s - %d/%d", pair, i + 1, len(pairs))
            self.one(pair, start_ts=start_ts, end_ts=end_ts, overwrite=overwrite)
            logger.info("%s - Finished - %.2fs", pair, timer() - t)
        self.db_short_info()

    def ring(self, pairs, every=TimePeriod.MINUTE.value, iterations=None):
        """
        Grabs the most recent data for a row of pairs on repeat

        Requires all pairs to be persistent in the database.

        :param pairs: list of pairs
        :param start_ts: timestamp of the start point
        :param overwrite: delete underlying collection before insert (requires start_ts)
        :param every: timestamp of the end point
        :param iterations: maximum number of times a grabber row is executed
        :return: None
"""
        logger.info("Ring - %d pairs - Every %s", len(pairs), td_format(timedelta(seconds=every)))
        iteration = 1
        while (True):
            logger.info("Row - Iteration %d", iteration)
            # Collect head every predefined amount of time
            self.row(pairs, end_ts=now_ts())
            iteration += 1
            if iterations is not None and iteration > iterations:
                break
            sleep(every)
        self.db_long_info()
