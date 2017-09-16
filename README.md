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
import plnxgrabber

polo = Poloniex()
client = MongoClient('localhost:27017')
db = client['TradeHistory']
grabber = plnxgrabber.Grabber(polo, db)
```

### Single pair, single action

For a single pair, drop previously stored history and collect the last minute:
```python
grabber.one('USDT_BTC',
            start_ts=plnxgrabber.ts_ago(60),
            end_ts=plnxgrabber.ts_now(),
            drop=True)
```

Considering we have history stored in db, extend it by newest records once:
```python
grabber.one('USDT_BTC', end_ts=plnxgrabber.ts_now())
```

### Multiple pairs, single action

For a row of pairs, drop previous history and collect the last 5 minutes:
```python
grabber.row(['USDT_BTC', 'USDT_ETH', 'USDT_LTC'],
            start_ts=plnxgrabber.ts_ago(300),
            end_ts=plnxgrabber.ts_now(),
            drop=True)
```

### Multiple pairs, repeating action

Grab last day of history for a row of pairs and keep updating every 60 sec:
```python
grabber.ring(["USDT_BTC", "USDT_ETH"], 
             start_ts=plnxgrabber.ts_ago(60*60*24), 
             drop=True, 
             every=60)
```
