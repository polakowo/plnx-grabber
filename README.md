![python](https://img.shields.io/badge/python-3-yellow.svg)![license](https://img.shields.io/badge/license-GPL%20v3-yellow.svg)

# plnx-grabber
Transfers trade history of any pair and time period from Poloniex exchange into the local MongoDB database

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

## Usage

### General

Get information on history available on Poloniex:
```python
grabber.remote_info(['USDT_BTC', 'USDT_ETH', 'USDT_LTC', 'USDT_BCH])
```

![alt text](/img/remote_info.png)

Get information on pairs stored locally:
```python
grabber.db_info()
```

![alt text](/img/db_info.png)

Get progress of currently stored history relative to overall available on Poloniex:
```python
grabber.progress()
```

![alt text](/img/progress.png)

### Grab one pair

To collect trade history for a single pair, use `Grabber.one()`

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

* If collection not empty, it gets extended
* Use `oldest` to auto-fill the timestamp of the oldest record in the collection
* Use `newest` to auto-fill the timestamp of the youngest record

Collect everything below the oldest record in the collection (backward):
```python
grabber.one('USDT_BTC', to_ts='oldest')

# or grabber.one('USDT_BTC', from_ts=0, to_ts='oldest')
```

Collect everything above the newest record in the collection (forward):
```python
grabber.one('USDT_BTC', from_ts='newest')

# or grabber.one('USDT_BTC', from_ts='newest', to_ts=plnxgrabber.now_ts())
```

Drop currently stored pair and recollect:
```python
grabber.one('USDT_BCH', from_ts='oldest', to_ts='newest', drop=True)
```

***

### Grab row of pairs

To collect trade history for a row of pairs, use `Grabber.row()`

For the following 4 pairs, collect the history from 1/9/2017 12:00:00 to 1/9/2017 18:00:00:
```python
from_ts = arrow.Arrow(2017, 9, 1, 12, 0, 0).timestamp
to_ts = arrow.Arrow(2017, 9, 1, 18, 0, 0).timestamp
grabber.row(['USDT_BTC', 'USDT_ETH', 'USDT_LTC', 'USDT_BCH'], from_ts=from_ts, to_ts=to_ts)
```

![UbIlti](https://i.makeagif.com/media/9-18-2017/UbIlti.gif)

* Pass 'ticker' instead of pair to perform an action on all pairs traded on Poloniex
* Pass 'db' to perform an action on all pairs stored locally
* Or use Regex

For each pair in the current ticker, collect the last 5 minutes:
```python
grabber.row('ticker', from_ts=plnxgrabber.ago_ts(5*60))
```

Recollect each collection:
```python
grabber.row('db', from_ts='oldest', to_ts='newest', drop=True)
```

For each ETH pair, collect the last minute:
```python
grabber.row(r'(ETH_+)', from_ts=plnxgrabber.ago_ts(60))
```

***

### Update row of pairs

To constantly collect the most recent records for a row of pairs, use `Grabber.ring()`

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
