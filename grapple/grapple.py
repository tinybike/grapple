#!/usr/bin/env python
"""Download the Ripple ledger from rippled.

Grapple extracts the ledger from rippled via websocket.  It starts at the
current ledger index, and walks backwards until it reaches the genesis ledger.
The genesis ledger index is set by default to 152370.

If you have previously run Grapple, data will only be collected from the
current ledger to the maximum ledger index previously recorded.  Just set
the "full" flag if you prefer to re-download the entire ledger.

Grapple can collect data from a local or remote rippled instance.  If you have
a local rippled instance running that has downloaded all or most of the ledger,
I strongly recommend doing local data collection.  Fetching data from Ripple's
public websocket is very slow!

Also resamples the transaction time series to create "Open-Hi-Lo-Close" data,
which can be useful for statistical tests, or simply for drawing charts.

Grapple is designed to integrate with PostgreSQL, using connection information
in config.py.  By default, it assumes that your database is located on
localhost (127.0.0.1), and that your database username, password, and password
are all "grapple".

(While this is certainly not the most secure setup, it may be convenient for
people who install Grapple via pip, and do not wish to edit its source code.)

Usage as a Python module:

    from grapple import Grapple

    grapple = Grapple()
    grapple.download()

Usage as a script:

    python grapple.py [-flags]

Optional flags:

    -w, --websocket [websocket url]:
        Specify the rippled websocket url. (default=ws://127.0.0.1:6006/)

    -p, --public:
        Use Ripple Labs' public websocket, wss://s1.ripple.com:51233.

    -f, --full:
        Download the full Ripple ledger.  Automatic on first run.

    -g, --genesis [ledger index]:
        Halting point for full downloads; ignored for partial downloads.

    -q, --quiet:
        Suppress command line output.

"""
from __future__ import division
import sys
try:
    import cdecimal
    sys.modules["decimal"] = cdecimal
except:
    pass
import getopt
import json
import websocket
from decimal import Decimal, getcontext, ROUND_HALF_EVEN
from contextlib import contextmanager
import pandas as pd
import pandas.io.sql as psql
import numpy as np
import psycopg2 as db
import psycopg2.extensions as ext
from psycopg2.extras import RealDictCursor
from config import *

getcontext().rounding = ROUND_HALF_EVEN

# Postgres connection
conn = db.connect(POSTGRES_CONNECTION_STRING)
conn.set_isolation_level(ext.ISOLATION_LEVEL_READ_COMMITTED)

class Grapple(object):

    def __init__(self, socket_url="ws://127.0.0.1:6006/", full=False,
                 genesis=152370, quiet=True, resampling_frequencies=('D',)):
        """
        Args:
          socket_url (str): rippled websocket URL (default=ws://127.0.0.1:6006/)
          full (bool): True if downloading the full ledger (starting from the
                       current ledger and walking back to the genesis ledger).
                       False if the download should stop at the last current
                       ledger (the last time grapple was run). (default=True)
          genesis (int): Genesis block index.  If full=True, this is where the
                         download ends; otherwise, this value is ignored.
                         (default=152370)
          quiet (bool): If True, suppress console output. (default=True)
          resampling_frequencies (tuple): Resampling frequencies, using pandas
                                          frequency codes.  If None, then
                                          resampling is disabled.
                                          (default=('D',) or daily)

        """
        self.full = full
        self.socket_url = socket_url
        self.start_date = None
        self.halt = genesis
        self.socket = None
        self.ledger_current_index = None
        self.ledgers_to_read = None
        self.updates = 0
        self.resampling_frequencies = resampling_frequencies

    def get_current_index(self, retry=False):
        try:
            if self.socket is not None:
                self.socket.send(json.dumps({'command': 'ledger_current'}))
                data = json.loads(self.socket.recv())
                if data and data['status'] == 'success':
                    if 'result' in data and 'ledger_current_index' in data['result']:
                        self.ledger_current_index = data['result']['ledger_current_index']
                        print "Current ledger index:", self.ledger_current_index
            else:
                self.get_current_index(retry=True)
        except Exception as e:
            print e
            if retry:
                return
            self.get_current_index(retry=True)

    def get_tx(self, tx_hash, data):
        try:
            if self.socket is not None:
                self.socket.send(json.dumps({
                    'command': 'tx',
                    'transaction': tx_hash,
                }))
                tx_data = self.socket.recv()
                tx_data = json.loads(tx_data)
                if tx_data['status'] == 'success' and 'result' in tx_data:
                    options = {
                        'ledger_time': data['result']['ledger']['close_time'],
                        'tx_hash': tx_hash,
                    }
                    return tx_data['result'], options
        except Exception as e:
            print e
            import pdb; pdb.set_trace()
        return False, False

    def parse_tx(self, tx, accepted, ledger_time=None, tx_hash=None):
        stored_tx_count = 0
        if tx['TransactionType'] == 'Payment' and 'meta' in tx and tx['meta']['TransactionResult'] == 'tesSUCCESS':
            for affected_node in tx['meta']['AffectedNodes']:
                if 'ModifiedNode' in affected_node:
                    node = affected_node['ModifiedNode']
                elif 'DeletedNode' in affected_node:
                    node = affected_node['DeletedNode']
                else:
                    continue
                is_offer = node['LedgerEntryType'] == 'Offer'
                has_prev = 'PreviousFields' in node
                if has_prev:
                    has_pays = 'TakerPays' in node['PreviousFields']
                    has_gets = 'TakerGets' in node['PreviousFields']
                if is_offer and has_prev and has_pays and has_gets:
                    previous = node['PreviousFields']
                    final = node['FinalFields']
                    adjust_xrp = 10**6
                    if 'currency' in final['TakerGets']:
                        gets = {
                            'currency': final['TakerGets']['currency'],
                            'amount': Decimal(previous['TakerGets']['value']) - Decimal(final['TakerGets']['value']),
                            'issuer': final['TakerGets']['issuer'],
                        }
                    else:
                        gets = {
                            'currency': 'XRP',
                            'amount': (Decimal(previous['TakerGets']) - Decimal(final['TakerGets'])) / adjust_xrp,
                            'issuer': None,
                        }
                    if 'currency' in final['TakerPays']:
                        pays = {
                            'currency': final['TakerPays']['currency'],
                            'amount': Decimal(previous['TakerPays']['value']) - Decimal(final['TakerPays']['value']),
                            'issuer': final['TakerPays']['issuer'],
                        }
                    else:
                        pays = {
                            'currency': 'XRP',
                            'amount': (Decimal(previous['TakerPays']) - Decimal(final['TakerPays'])) / adjust_xrp,
                            'issuer': None,
                        }
                    gets_idx, pays_idx = None, None
                    if gets['amount'] > 0 and pays['amount'] > 0:
                        pays['quantum'] = Decimal(currency_precision(pays['currency']))
                        pays['price'] = (gets['amount'] / pays['amount']).quantize(pays['quantum'])
                        gets['quantum'] = Decimal(currency_precision(gets['currency']))
                        gets['price'] = (pays['amount'] / gets['amount']).quantize(gets['quantum'])
                        pays['amount'] = pays['amount'].quantize(pays['quantum'])
                        gets['amount'] = gets['amount'].quantize(gets['quantum'])
                        try:
                            with cursor() as cur:
                                txdate = None if ledger_time is None else ledger_time + RIPPLE_EPOCH
                                records = {
                                    'txid': tx['meta']['TransactionIndex'],
                                    'txhash': tx_hash,
                                    'market': pays['currency'] + gets['currency'],
                                    'currency1': pays['currency'],
                                    'currency2': gets['currency'],
                                    'amount1': pays['amount'],
                                    'amount2': gets['amount'],
                                    'price1': pays['price'],
                                    'price2': gets['price'],
                                    'issuer1': pays['issuer'],
                                    'issuer2': gets['issuer'],
                                    'account1': final['Account'],
                                    'txdate': txdate,
                                    'ledgerindex': tx['ledger_index'],
                                    'accepted': accepted,
                                }
                                sql = (
                                    "INSERT INTO ripple_ledger "
                                    "(txid, txhash, market, currency1, currency2, amount1, amount2, "
                                    "price1, price2, issuer1, issuer2, account1, "
                                    "txdate, ledgerindex, accepted, collected) "
                                    "VALUES "
                                    "(%(txid)s, %(txhash)s, %(market)s, %(currency1)s, %(currency2)s, "
                                    "%(amount1)s, %(amount2)s, %(price1)s, %(price2)s, "
                                    "%(issuer1)s, %(issuer2)s, %(account1)s, "
                                    "%(txdate)s, %(ledgerindex)s, %(accepted)s, now())"
                                )
                                cur.execute(sql, records)
                                stored_tx_count += 1
                        except Exception as exc:
                            print exc
                            import pdb; pdb.set_trace()
        return stored_tx_count

    def parse_ledger(self, data):
        accepted = False
        tx_hash_list = None
        if 'result' in data and 'ledger' in data['result'] and \
                        'transactions' in data['result']['ledger']:
            tx_hash_list = data['result']['ledger']['transactions']
            if 'accepted' in data['result']['ledger'] and \
                    data['result']['ledger']['accepted']:
                accepted = True
        return tx_hash_list, accepted

    def read_next_ledger(self):
        if self.socket is not None:
            self.socket.send(json.dumps({
                'command': 'ledger',
                'ledger_index': self.ledger_index,
                'transactions': True,
                'expand': False,
            }))
            ledger = self.socket.recv()
            return json.loads(ledger)
    
    def rippled_connect(self):
        for i in xrange(5):
            try:
                self.socket = websocket.create_connection(self.socket_url)
                print "Connected to", self.socket_url, "(attempt", str(i+1) + ")"
                return True
            except ValueError as e:
                print "Error connecting to rippled", e
        return False

    def is_duplicate(self, tx_hash):
        duplicate = False
        with cursor() as cur:
            query = "SELECT count(*) FROM ripple_ledger WHERE txhash = %s"
            cur.execute(query, (tx_hash,))
            for row in cur:
                duplicate = row[0]
        return duplicate

    def resampler(self, df, freq='D'):
        df.txdate = pd.to_datetime(df.txdate, unit='s')
        df = df.set_index(df.txdate)
        rs = [df.price1.resample(freq, how='ohlc'),
              df.price2.resample(freq, how='ohlc')]
        for i, r in enumerate(rs):
            idx = str(i + 1)
            rs[i] = r.join(
                df['amount'+idx].resample(freq, how='sum'), on=r.index).join(
                df['price'+idx].resample(freq, how='median'), on=r.index
            )
        rs = rs[0].join(rs[1], on=rs[0].index, lsuffix=1, rsuffix=2)
        rs.index = rs.index.astype(np.int64) // 10**9
        return rs

    def write_resampled(self, rs, market, cur, freq='D'):
        nrows = rs.shape[0]
        for i in xrange(nrows):
            row = [rs.index[i], freq, market[0], market[1]]
            vals = rs[i:i+1].values.flatten().tolist()
            if np.isnan(np.sum(vals)):
                continue
            vals = [Decimal(v).quantize(Decimal('.00000001')) for v in vals]
            row.extend(vals)
            valuestr = ','.join(['%s'] * len(row))
            query = (
                "INSERT INTO resampled_ledger "
                "(starttime, freq, currency1, currency2, open1, high1, low1, "
                "close1, volume1, price1, open2, high2, low2, close2, "
                "volume2, price2) "
                "VALUES (%s)"
            ) % valuestr
            cur.execute(query, tuple(row))
            self.updates += 1

    def resample_time_series(self):
        """OHLC time series resampler.
        
        Resamples time series data to create Open-Hi-Lo-Close (OHLC) data,
        which can be useful for statistical tests, or simply for charting.

        Frequency abbreviations are taken from the pandas library.  By
        default, this method does daily ("D") resampling.

        """
        freq = self.resampling_frequencies
        with cursor() as cur:
            cur.execute("SELECT max(starttime) FROM resampled_ledger")
            if cur.rowcount:
                last_resample = str(cur.fetchone()[0])
            else:
                last_resample = 0
            print "Resampling time series..."
            for market in self.currency_pairs():
                sys.stdout.write(market[0] + " " + market[1] + "\r")
                sys.stdout.flush()

                # Resample all transactions
                if self.full:
                    query = (
                        "SELECT currency1, currency2, price1, price2, "
                        "amount1, amount2, txdate FROM ripple_ledger "
                        "WHERE market = '%s' "
                        "ORDER BY txdate"
                    ) % (market[0] + market[1])

                # Resample transactions from the last resampling
                # starting timestamp or newer
                else:
                    query = (
                        "SELECT currency1, currency2, price1, price2, "
                        "amount1, amount2, txdate FROM ripple_ledger "
                        "WHERE market = '%s' AND txdate >= '%s' "
                        "ORDER BY txdate"
                    ) % ((market[0] + market[1]), last_resample)
                df = psql.frame_query(query, conn)
                if not df.empty:
                    for f in freqs:
                        rs = self.resampler(df, freq=f)
                        self.write_resampled(rs, market, cur, freq=f)
                    conn.commit()
            print
            print self.updates, "resampled_ledger records updated"
            print

        # Index the columns: starttime, freq, currency1, currency2
        conn.set_isolation_level(ext.ISOLATION_LEVEL_AUTOCOMMIT)
        with cursor() as cur:
            print "Indexing..."
            idx_queries = (
                "DROP INDEX IF EXISTS idx_ledger_interval",
                (
                    "CREATE INDEX CONCURRENTLY idx_ledger_interval ON "
                    "resampled_ledger(starttime, freq, currency1, currency2)"
                ),
            )
            for query in idx_queries:
                cur.execute(query)

    def housekeeping(self):
        queries = (
            "DROP TABLE IF EXISTS ripple_ledger CASCADE",
            "DROP TABLE IF EXISTS resampled_ledger CASCADE",
            (
                "CREATE TABLE resampled_ledger ("
                "starttime bigint,"
                "freq varchar(10),"
                "currency1 varchar(10),"
                "currency2 varchar(10),"
                "open1 numeric(24,8),"
                "open2 numeric(24,8),"
                "high1 numeric(24,8),"
                "high2 numeric(24,8),"
                "low1 numeric(24,8),"
                "low2 numeric(24,8),"
                "close1 numeric(24,8),"
                "close2 numeric(24,8),"
                "volume1 numeric(24,8),"
                "volume2 numeric(24,8),"
                "medprice1 numeric(24,8))"
            ), (
                "CREATE TABLE ripple_ledger ("
                "internalid bigserial NOT NULL PRIMARY KEY,"
                "txid bigint,"
                "txhash varchar(1000),"
                "market varchar(20),"
                "currency1 varchar(10),"
                "currency2 varchar(10),"
                "price1 numeric(24,8),"
                "price2 numeric(24,8),"
                "amount1 numeric(24,8),"
                "amount2 numeric(24,8),"
                "issuer1 varchar(1000),"
                "issuer2 varchar(1000),"
                "account1 varchar(1000),"
                "account2 varchar(1000),"
                "txdate bigint,"
                "ledgerindex bigint,"
                "accepted boolean,"
                "collected timestamp DEFAULT statement_timestamp())"
            ),
        )
        with cursor() as cur:
            for query in queries:
                cur.execute(query)
                conn.commit()

    def find_target_ledger(self):
        with cursor() as cur:
            cur.execute("SELECT max(ledgerindex) FROM ripple_ledger")
            for row in cur:
                max_ledgerindex = row[0]
            if max_ledgerindex is not None:
                self.halt = int(max_ledgerindex)

    def rippled_history(self):
        if self.rippled_connect():
            self.get_current_index()
            if not self.full:
                self.find_target_ledger()
            print "Reading from ledger", self.ledger_current_index, "to", self.halt
            self.ledgers_to_read = self.ledger_current_index - self.halt
            self.ledger_index = self.ledger_current_index - 1
            self.stored_tx = 0
            while self.ledger_index >= self.halt:
                try:
                    ledger = self.read_next_ledger()
                    ledgers_read = self.ledger_current_index - self.ledger_index - 1
                    progress = round(float(ledgers_read) / float(self.ledgers_to_read), 3)
                    sys.stdout.write("Read " + str(ledgers_read) + "/" +\
                                     str(self.ledgers_to_read) + " [" +\
                                     str(progress * 100) + "%] ledgers (" +\
                                     str(self.stored_tx) + " transactions)\r")
                    sys.stdout.flush()
                    if ledger is not None:
                        tx_hash_list, accepted = self.parse_ledger(ledger)
                        if tx_hash_list is not None:
                            for tx_hash in tx_hash_list:
                                if not self.full and self.ledger_index == self.halt:
                                    if self.is_duplicate(tx_hash):
                                        continue
                                tx_data_result, options = self.get_tx(tx_hash, ledger)
                                if tx_data_result:
                                    self.stored_tx += self.parse_tx(tx_data_result,
                                                                    accepted,
                                                                    **options)
                    self.ledger_index -= 1
                except Exception as exc:
                    print exc
            self.socket.close()
            print
            return True
        return False

    def download(self):
        """
        Walk from the current ledger index to the genesis ledger index,
        and download transactions from rippled.
        """
        self.housekeeping()
        self.rippled_history()
        if self.resampling_frequencies is not None:
            self.resample_time_series()


@contextmanager
def cursor(cursor_factory=False):
    """Database cursor generator. Commit on context exit."""
    try:
        if cursor_factory:
            cur = conn.cursor(cursor_factory=RealDictCursor)
        else:
            cur = conn.cursor()
        yield cur
    except (db.Error, Exception) as e:
        cur.close()
        if conn:
            conn.rollback()
        print e.message
        raise
    else:
        conn.commit()
        cur.close()

def currency_precision(currency_code):
    if currency_code.upper() == 'NXT':
        precision = '.01'
    elif currency_code.upper() == 'XRP':
        precision = '.000001'
    else:
        precision = '.00000001'
    return precision

def main(argv=None):
    if argv is None:
        argv = sys.argv
    try:
        short_opts = 'hrfqw:g:'
        long_opts = ['help', 'remote', 'full', 'quiet', 'websocket=', 'g=']
        opts, vals = getopt.getopt(argv[1:], short_opts, long_opts)
    except getopt.GetoptError as e:
        print >>sys.stderr, e.msg
        print >>sys.stderr, "for help use --help"
        return 2
    parameters = {
        'full': False,
        'genesis': 152370,
        'quiet': False,
    }
    for opt, arg in opts:
        if opt in ('-h', '--help'):
            print __doc__
            return 0
        elif opt in ('-p', '--public'):
            parameters['socket_url'] = RIPPLE_PUBLIC_WEBSOCKET
        elif opt in ('-f', '--full'):
            parameters['full'] = True
        elif opt in ('-q', '--quiet'):
            parameters['quiet'] = True
        elif opt in ('-w', '--websocket'):
            parameters['socket_url'] = arg
        elif opt in ('-g', '--genesis'):
            parameters['genesis'] = arg
    
    Grapple(**parameters).download()

    try:
        if conn:
            conn.close()
    except:
        pass

if __name__ == '__main__':
    sys.exit(main())
