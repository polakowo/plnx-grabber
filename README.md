# plnx-grabber
Grabs trade history from Poloniex exchange and chunk-wise inserts into MongoDB

## Installation

```
pip install https://github.com/polakowo/plnx-grabber/archive/master.zip
```

## Usage

### Basic Setup

```python
from poloniex import Poloniex
from pymongo import MongoClient
import arrow
import plnxgrabber

polo = Poloniex()
client = MongoClient('localhost:27017')
db = client['TradeHistory']
grabber = plnxgrabber.Grabber(polo, db)
```

### Single pair, single action

#### Collection empty yet

Fetch the entire history for a pair of symbols:
```python
grabber.one('USDT_BCH')

# or grabber.one('USDT_BCH', start_ts=0, end_ts=plnxgrabber.ts_now())
```

Fetch the history of a period of time:
```python
start_ts = arrow.Arrow(2017, 9, 1, 0, 0, 0).timestamp
end_ts = arrow.Arrow(2017, 9, 1, 0, 5, 0).timestamp
grabber.one('USDT_BTC', start_ts=start_ts, end_ts=end_ts)
```

Collect the last hour:
```python
grabber.one('USDT_BTC', start_ts=plnxgrabber.ts_ago(60*60))

# or grabber.one('USDT_BTC', start_ts=plnxgrabber.ts_ago(60*60), end_ts=plnxgrabber.ts_now())
```

#### Collection not empty

If no `overwrite` parameter passed, extend previously populated collection.

Extend the collection's upper bound by latest records:
```python
grabber.one('USDT_BTC', start_ts='upper')

# or grabber.one('USDT_BTC', start_ts='upper', end_ts=plnxgrabber.now_ts())
```

Extend the collection's lower bound by earliest records:
```python
grabber.one('USDT_BTC', end_ts='lower')

# or grabber.one('USDT_BTC', start_ts=0, end_ts='lower')
```

Extend both collection's bounds:
```python
grabber.one('USDT_BTC')

# or grabber.one('USDT_BTC', start_ts=0, end_ts=plnxgrabber.now_ts())
```

Extend both collection's bounds to completely fill a period:
```python
start_ts = arrow.Arrow(2017, 1, 1, 0, 0, 0).timestamp
end_ts = arrow.Arrow(2017, 9, 1, 0, 0, 0).timestamp
grabber.one('USDT_BTC', start_ts=start_ts, end_ts=end_ts)
```

***Important**: Algorithm prevents building gaps in collections. If the history stored in collection and the one fetched from Poloniex build a gap in between, it gets filled automatically by extending start_ts or end_ts accordingly. See comments for further details.*

If `overwrite` parameter passed, overwrite collection completely.

Fill the collection's bounds again:
```python
grabber.one('USDT_BCH', start_ts='lower', end_ts='upper', overwrite=True)
```

### Multiple pairs, single action

For a row of pairs, collect the last 5 minutes:
```python
grabber.row(['USDT_BTC', 'USDT_ETH', 'USDT_LTC'], start_ts=plnxgrabber.ts_ago(5*60))
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

***Important**: Ring only updates, and requires collections to be non-empty. If you want to collect history for a row of pairs and then update them every predefined amount of time, first execute a row and then a ring. See comments for further details.*
