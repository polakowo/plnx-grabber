import logging
from timeit import default_timer as timer

import arrow
import pandas as pd
import pymongo

# Logger
############################################################

logger = logging.getLogger(__name__)
# No logging by default
logger.addHandler(logging.NullHandler())


# Series information
############################################################

def ts_to_date(ts):
    return arrow.get(ts)


def ts_to_str(ts, fmt='ddd DD/MM/YYYY HH:mm:ss ZZ'):
    return arrow.get(ts).format(fmt)


def ts_delta(ts1, ts2):
    return abs(ts_to_date(ts1) - ts_to_date(ts2))


def format_delta(td_object):
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


def format_bytes(num):
    for x in ['B', 'KB', 'MB', 'GB', 'TB']:
        if num < 1024.0:
            return '%3.1f %s' % (num, x)
        num /= 1024.0


def series_info_str(series_info):
    return "{ %s : %d, %s : %d, %s, %d rows, %s }" % (
        ts_to_str(series_info['from_ts']),
        series_info['from_id'],
        ts_to_str(series_info['to_ts']),
        series_info['to_id'],
        format_delta(series_info['delta']),
        series_info['count'],
        format_bytes(series_info['memory']))


# Dataframes
############################################################

def df_to_docs(df):
    # Convert df into shape suitable for export into MongoDB
    return df.reset_index().to_dict(orient='records')


def docs_to_df(docs, new_index=['_id', 'ts']):
    # Convert docs to df
    return pd.DataFrame(list(docs)).set_index(new_index, drop=True)


# MongoTS
############################################################

class MongoTS(object):
    """
    Wrapper around pymongo for dealing with trade series information
    """

    def __init__(self, db):
        # Set running MongoDB instance
        self.db = db

    def db_info(self):
        # Aggregates basic info on current state of db
        cname_series_info = {cname: self.series_info(cname) for cname in self.list_cols()}
        logger.info("Database '{0}' - {1} collections - {2:,} documents - {3}"
                    .format(self.db.name,
                            len(cname_series_info),
                            sum(series_info['count'] for series_info in cname_series_info.values()),
                            format_bytes(sum(series_info['memory'] for series_info in cname_series_info.values()))))
        # Shows detailed descriptions of each collection
        for cname, series_info in cname_series_info.items():
            logger.info("%s - %s", cname, series_info_str(series_info))

    def clear_db(self):
        # Drop all collections
        for cname in self.list_cols():
            self.drop_col(cname)

    # Collections

    def list_cols(self):
        return self.db.collection_names()

    def create_col(self, cname):
        # Create new collection and index on timestamp field
        self.db.create_collection(cname)
        self.db[cname].create_index([('ts', pymongo.ASCENDING)], unique=False, background=True)

    def drop_col(self, cname):
        # Delete collection entirely
        self.db[cname].drop()
        logger.debug("%s - Dropped entirely", cname)

    def col_exists(self, cname):
        return cname in self.list_cols()

    def col_non_empty(self, cname):
        # Check whether collection exists and not empty
        return self.col_exists(cname) and self.docs_count(cname) > 0

    def col_memory(self, cname):
        # Returns size of all documents + header + index size
        return self.db.command('collstats', cname)['size'] + 16 * 100 + self.db.command('collstats', cname)[
            'totalIndexSize']

    # Series

    def series_info(self, cname):
        # Returns the most important series information
        # (start and end points, their delta, num of rows and memory taken)
        from_dict = self.from_doc(cname)
        to_dict = self.to_doc(cname)
        return {
            'from_ts': from_dict['ts'],
            'from_id': from_dict['_id'],
            'to_ts': to_dict['ts'],
            'to_id': to_dict['_id'],
            'delta': ts_delta(from_dict['ts'], to_dict['ts']),
            'count': self.docs_count(cname),
            'memory': self.col_memory(cname)}

    def verify_series(self, cname):
        # Verifies the incremental nature of trade id across series
        t = timer()
        series_info = self.series_info(cname)
        diff = series_info['count'] - (series_info['to_id'] - series_info['from_id'] + 1)
        if diff > 0:
            logger.warning("Collection - Found duplicates (%d) - %.2fs", diff, timer() - t)
        elif diff < 0:
            logger.warning("Collection - Found gaps (%d) - %.2fs", abs(diff), timer() - t)
        else:
            logger.debug("Collection - Verified - %.2fs", timer() - t)
        return diff == 0

    def series_range(self, cname, from_ts, to_ts):
        # Get the series in the range
        return self.find_docs(cname, query={'ts': {'$gte': from_ts, '$lte': to_ts}})

    def series_candlesticks(self, cname, from_ts, to_ts):
        # Group the data into candlesticks
        # TODO: Switch from timestamps to dates for aggregation to work
        return self.db.command('aggregate', cname,
            pipeline=[
                {'$project': {
                    'minute': {
                        '0': {'$year': '$ts'},
                        '1': {'$month': '$ts'},
                        '2': {'$dayOfMonth': '$ts'},
                        '3': {'$hour': '$ts'},
                        '4': {'$minute': '$ts'}
                      },
                     'ts': 1,
                     'rate': 1
                    }
                  },
                  {'$sort': {'ts': 1}},
                  {'$group': {
                      '_id': '$minute',
                      'ts': {'$first': '$ts'},
                      'rate_open': {'$first': '$rate'},
                      'rate_close': {'$last': '$rate'},
                      'rate_high': {'$max': '$rate'},
                      'rate_low': {'$min': '$rate'},
                      'rate_avg': {'$avg': '$rate'}
                    }
                  },
                  {'$sort': {'ts': 1}},
                  {'$project': {
                      '_id': '$ts',
                      'rate': {
                        'open': '$rate_open',
                        'close': '$rate_close',
                        'high': '$rate_high',
                        'low': '$rate_low',
                        'avg': '$rate_avg'
                      }
                    }
                  },
            ])['result']

    # Documents

    def docs_count(self, cname):
        # Documents count in collection
        return self.db.command('collstats', cname)['count']

    def from_doc(self, cname):
        # Return the document for the earliest point in series
        return next(self.db[cname].find().sort([['_id', 1]]).limit(1))

    def to_doc(self, cname):
        # Return the document for the most recent point in series
        return next(self.db[cname].find().sort([['_id', -1]]).limit(1))

    def insert_docs(self, cname, docs):
        # Convert df into list of dicts and insert into collection (fast)
        t = timer()
        result = self.db[cname].insert_many(docs)
        logger.debug("%s - Collection - Inserted %d documents - %.2fs",
                     cname, len(result.inserted_ids), timer() - t)

    def update_docs(self, cname, docs):
        # Convert df into list of dicts and only insert records not present in the collection (slow)
        t = timer()
        n_modified = 0
        n_upserted = 0
        for record in docs:
            result = self.db[cname].update_one(
                {'_id': record['_id']},
                {'$setOnInsert': record},
                upsert=True)
            if result.modified_count is not None and result.modified_count > 0:
                n_modified += result.modified_count
            if result.upserted_id is not None:
                n_upserted += 1
        logger.debug("%s - Collection - Modified %d, upserted %d documents - %.2fs",
                     cname, n_modified, n_upserted, timer() - t)

    def delete_docs(self, cname, query={}):
        # Delete documents
        t = timer()
        result = self.db[cname].delete_many(query)
        logger.debug("%s - Collection - Deleted %d documents - %.2fs",
                     cname, result.deleted_count, timer() - t)

    def find_docs(self, cname, query={}):
        # Return generator for documents which match query
        return self.db[cname].find(query)
