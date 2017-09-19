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

### Single pair, single action

#### 1) Collection empty yet

- If `from_ts` is not passed, it gets filled by 0.
- If `end_ts` is not passed, it gets filled by current time.

Fetch the entire history:
```python
grabber.one('USDT_BCH')

# or grabber.one('USDT_BCH', from_ts=0, to_ts=plnxgrabber.ts_now())
```

Fetch the history for a time period:
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

If no `overwrite` parameter passed, extend previously populated collection either by newer records (keep trade history up to date), or by older records (if any available).

Extend both collection's end to completely fill a time period:
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

- Use `newest` to auto-fill the timestamp of the youngest record in the collection
- Use `oldest` to auto-fill the timestamp of the oldest record in the collection
- If none of them is passed, extend collection automatically (from one or both ends)

Start from the newest record in the collection and extend toward now:
```python
grabber.one('USDT_BTC', from_ts='newest')

# or grabber.one('USDT_BTC', from_ts='newest', to_ts=plnxgrabber.now_ts())
```

Start from the beginning of pair *USDT_BTC* and grab everything toward the oldest record in the collection:
```python
grabber.one('USDT_BTC', to_ts='oldest')

# or grabber.one('USDT_BTC', from_ts=0, to_ts='oldest')
```

***Important**: Algorithm prevents building gaps in collections. If the history stored in collection and the one fetched from Poloniex build a gap in between, it gets filled automatically by extending from_ts or to_ts accordingly. See comments for further details.*

If `overwrite` parameter passed, overwrite collection completely.

Recollect the currently stored *USDT_BCH* pair:
```python
grabber.one('USDT_BCH', from_ts='oldest', to_ts='newest', overwrite=True)
```

### Multiple pairs, single action

For each pair in a row, collect history of a period of time:
```python
from_ts = arrow.Arrow(2017, 9, 1, 12, 0, 0).timestamp
to_ts = arrow.Arrow(2017, 9, 1, 18, 0, 0).timestamp
grabber.row(['USDT_BTC', 'USDT_ETH', 'USDT_LTC', 'USDT_BCH'], from_ts=from_ts, to_ts=to_ts, overwrite=True)
```

![UbIlti](https://i.makeagif.com/media/9-18-2017/UbIlti.gif)

For each pair from ticker returned by Poloniex, collect last 5 minutes:
```python
grabber.row('ticker', from_ts=plnxgrabber.ago_ts(5*60), overwrite=True)
```

Recollect each pair currently stored in db:
```python
grabber.row('db', from_ts='lower', to_ts='upper', overwrite=True)
```

### Multiple pairs, repeating action

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

Continue updating all pairs currently stored in db:
```python
grabber.ring('db')
```

***Important**: Ring only updates, and requires collections to be non-empty. If you want to collect history for a row of pairs and then update them every predefined amount of time, first execute a row and then a ring. See comments for further details.*
