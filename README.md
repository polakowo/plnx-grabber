# plnx-grabber
Grabs trade history from Poloniex exchange and chunk-wise inserts into MongoDB

## Installation

```
pip install https://github.com/polakowo/plnx-grabber/archive/master.zip
```

## Setup

```python
from pymongo import MongoClient
import arrow
import plnxgrabber

logging.basicConfig(format='%(asctime)s - %(name)s - %(funcName)s() - %(levelname)s - %(message)s',
                    datefmt='%d/%m/%Y %H:%M:%S',
                    level=logging.INFO)
# or your preferred logger

client = MongoClient('localhost:27017')
db = client['TradeHistory']
# or your preferred db name
grabber = plnxgrabber.Grabber(db)
```

## How-To

### One pair

To perform an action on a single pair, use `Grabber.one()`

#### 1) Collection empty yet

* If collection empty, program will simply record everything
* `from_ts` and `end_ts` are either timestamps or strings (see below)
* If `from_ts` is not passed, it gets filled by 0
* If `to_ts` is not passed, it gets filled by current time

Collect the entire history:
```python
grabber.one('USDT_BCH')

# or grabber.one('USDT_BCH', from_ts=0, to_ts=plnxgrabber.ts_now())
```

Collect the history between 1/9/2017 12:00:00 to 1/9/2017 18:00:00:
```python
from_ts = arrow.Arrow(2017, 9, 1, 12, 0, 0).timestamp
to_ts = arrow.Arrow(2017, 9, 1, 18, 0, 0).timestamp
grabber.one('USDT_BTC', from_ts=from_ts, to_ts=to_ts)
```

Collect the last hour:
```python
grabber.one('USDT_BTC', from_ts=plnxgrabber.ts_ago(60*60))

# or grabber.one('USDT_BTC', from_ts=plnxgrabber.ts_ago(60*60), to_ts=plnxgrabber.ts_now())
```

#### 2) Collection not empty

* Collections in MongoDB are named by their pairs
* If no `overwrite` parameter passed, extend the collection either by newer or older records

Extend both collection's ends to completely fill a time period:
```python
from_ts = arrow.Arrow(2017, 1, 1, 0, 0, 0).timestamp
to_ts = arrow.Arrow(2017, 9, 1, 0, 0, 0).timestamp
grabber.one('USDT_BTC', from_ts=from_ts, to_ts=to_ts)
```

Complete collection from both its ends:
```python
grabber.one('USDT_BTC')

# or grabber.one('USDT_BTC', from_ts=0, to_ts=plnxgrabber.now_ts())
```

* Use `oldest` to auto-fill the timestamp of the oldest record in the collection
* Use `newest` to auto-fill the timestamp of the youngest record
* If none of them is passed, extend collection automatically (from one or both ends)

Extend the collection by older records (backward):
```python
grabber.one('USDT_BTC', to_ts='oldest')

# or grabber.one('USDT_BTC', from_ts=0, to_ts='oldest')
```

Extend the collection by newer records (forward):
```python
grabber.one('USDT_BTC', from_ts='newest')

# or grabber.one('USDT_BTC', from_ts='newest', to_ts=plnxgrabber.now_ts())
```

***Important**: Algorithm prevents building gaps in collections. If the history stored in collection and the one fetched from Poloniex build a gap in between, it gets filled automatically by extending from_ts or to_ts accordingly. Gaps are tested by running a consistency check on a trade id field.*

* If `overwrite` parameter passed, overwrite collection completely

Recollect the currently stored pair:
```python
grabber.one('USDT_BCH', from_ts='oldest', to_ts='newest', overwrite=True)
```

### Row of pairs

To perform an action on multiple pairs sequentially, use `Grabber.row()`

For the following 4 pairs, collect the history from 1/9/2017 12:00:00 to 1/9/2017 18:00:00:
```python
from_ts = arrow.Arrow(2017, 9, 1, 12, 0, 0).timestamp
to_ts = arrow.Arrow(2017, 9, 1, 18, 0, 0).timestamp
grabber.row(['USDT_BTC', 'USDT_ETH', 'USDT_LTC', 'USDT_BCH'], from_ts=from_ts, to_ts=to_ts, overwrite=True)
```

![UbIlti](https://i.makeagif.com/media/9-18-2017/UbIlti.gif)

* Pass 'ticker' instead of pair to perform an action on all pairs traded on Poloniex
* Pass 'db' to perform an action on all pairs stored locally
* Or even use Regex

For each pair in the current ticker, collect the last 5 minutes:
```python
grabber.row('ticker', from_ts=plnxgrabber.ago_ts(5*60), overwrite=True)
```

Recollect each collection:
```python
grabber.row('db', from_ts='oldest', to_ts='newest', overwrite=True)
```

For each ETH pair, collect the last minute:
```python
grabber.row('(ETH_+)', from_ts=plnxgrabber.ago_ts(60), overwrite=True)
```

### Ring of pairs

To constantly collect the most recent records, use `Grabber.ring()`

Keep updating a row of pairs every 60 sec:
```python
grabber.ring(["USDT_BTC", "USDT_ETH"], every=60)
```

Update a row of pairs 3 times:
```python
grabber.ring(["USDT_BTC", "USDT_ETH"], every=60, iterations=3)
```

Update continuously:
```python
grabber.ring(["USDT_BTC", "USDT_ETH"])
```

Continue updating all collections:
```python
grabber.ring('db')
```

***Important**: Ring only updates, and requires collections to be non-empty. If you want to collect history for a row of pairs and then update them every predefined amount of time, first execute a row and then a ring.*

**See comments for further details.**
