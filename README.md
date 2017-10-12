![python](https://img.shields.io/badge/python-3-yellow.svg)![license](https://img.shields.io/badge/license-GPL%20v3-yellow.svg)

# plnx-grabber
Transfer trade history from Poloniex into a local MongoDB database

* Every pair and time period
* Chunk wise without using much RAM
* One time, multiple times or continuous
* Smart outputs on history available on Poloniex as well as locally

## Installation

```
pip install https://github.com/polakowo/plnx-grabber/archive/master.zip
```

## Setup

```python
from plnxgrabber import plnxgrabber, mongots
from pymongo import MongoClient
import logging

logging.basicConfig(format='%(asctime)s - %(name)s - %(funcName)s() - %(levelname)s - %(message)s',
                    datefmt='%d/%m/%Y %H:%M:%S',
                    level=logging.INFO)
# or your preferred logger

client = MongoClient('localhost:27017')
db = client['TradeHistory']
mongo_ts = mongots.MongoTS(db)
grabber = plnxgrabber.Grabber(mongo_ts)
```

## Usage

### General

Get information on history available on Poloniex:
```python
grabber.remote_info(['USDT_BTC', 'USDT_ETH', 'USDT_LTC', 'USDT_BCH])
```

Get information on pairs stored locally:
```python
grabber.db_info()
```

Get progress of currently stored history relative to overall available on Poloniex:
```python
grabber.progress()
```

### Grab one pair

To collect trade history for a single pair, use `Grabber.one()`

Collect the entire history:
```python
grabber.one('USDT_BCH')

# or grabber.one('USDT_BCH', from_dt=plnxgrabber.begin(), to_dt=plnxgrabber.now())
```

Collect the history between 1/9/2017 12:00:00 to 1/9/2017 18:00:00:
```python
from datetime import datetime
import pytz

from_dt = datetime(2017, 9, 1, 12, 0, 0, tzinfo=pytz.utc)
to_dt = datetime(2017, 9, 1, 18, 0, 0, tzinfo=pytz.utc)
grabber.one('USDT_BTC', from_dt=from_dt, to_dt=to_dt)
```

Collect the last hour:
```python
grabber.one('USDT_BTC', from_dt=plnxgrabber.ago(hours=1))

# or grabber.one('USDT_BTC', from_dt=plnxgrabber.ago(hours=1), to_dt=plnxgrabber.now())
```

* If collection not empty, it gets extended
* Use `oldest` to auto-fill the timestamp of the oldest record in the collection
* Use `newest` to auto-fill the timestamp of the youngest record

Collect everything below the oldest record in the collection (backward):
```python
grabber.one('USDT_BTC', to_dt='oldest')

# or grabber.one('USDT_BTC', from_dt=plnxgrabber.begin(), to_dt='oldest')
```

Collect everything above the newest record in the collection (forward):
```python
grabber.one('USDT_BTC', from_dt='newest')

# or grabber.one('USDT_BTC', from_dt='newest', to_dt=plnxgrabber.now())
```

Drop currently stored pair and recollect:
```python
grabber.one('USDT_BCH', from_dt='oldest', to_dt='newest', drop=True)
```

***

### Grab row of pairs

To collect trade history for a row of pairs, use `Grabber.row()`

For the following 4 pairs, collect the history from 1/9/2017 12:00:00 to 1/9/2017 18:00:00:
```python
from_dt = datetime(2017, 9, 1, 12, 0, 0, tzinfo=pytz.utc)
to_dt = datetime(2017, 9, 1, 18, 0, 0, tzinfo=pytz.utc)
grabber.row(['USDT_BTC', 'USDT_ETH', 'USDT_LTC', 'USDT_BCH'], from_dt=from_dt, to_dt=to_dt)
```

* Pass 'ticker' instead of pair to perform an action on all pairs traded on Poloniex
* Pass 'db' to perform an action on all pairs stored locally
* Or use Regex

For each pair in the current ticker, collect the last 5 minutes:
```python
grabber.row('ticker', from_dt=plnxgrabber.ago(minutes=5))
```

Recollect each collection:
```python
grabber.row('db', from_dt='oldest', to_dt='newest', drop=True)
```

For each ETH pair, collect the last minute:
```python
grabber.row(r'(ETH_+)', from_dt=plnxgrabber.ago(minutes=1))
```

***

### Update row of pairs

To constantly collect the most recent records for a row of pairs, use `Grabber.ring()`

Keep updating a row of pairs every 60 sec:
```python
grabber.ring(["USDT_BTC", "USDT_ETH"], every=60)
```

Update continuously:
```python
grabber.ring(["USDT_BTC", "USDT_ETH"])
```

Continue updating all collections:
```python
grabber.ring('db')
```
