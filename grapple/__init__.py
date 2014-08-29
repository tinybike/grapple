#!/usr/bin/env python
"""
Downloads data from rippled via websocket.

(c) Jack Peterson (jack@tinybike.net), 12/10/2013
"""
from __future__ import division
import sys
try:
    import cdecimal
    sys.modules["decimal"] = cdecimal
except:
    pass
import os
import getopt
import json
import csv
import time
import platform
import websocket
import requests
from decimal import Decimal, getcontext, ROUND_HALF_EVEN
from contextlib import contextmanager
import pandas as pd
import pandas.io.sql as psql
import numpy as np
try:
    import psycopg2 as db
    import psycopg2.extensions as ext
    from psycopg2.extras import RealDictCursor
except:
    import psycopg2cffi as db
    import psycopg2cffi.extensions as ext
    from psycopg2cffi.extras import RealDictCursor

__title__      = "grapple"
__version__    = "0.1"
__author__     = "Jack Peterson"
__copyright__  = "Copyright 2014, Jack Peterson"
__license__    = "MIT"
__maintainer__ = "Jack Peterson"
__email__      = "jack@tinybike.net"

getcontext().prec = 28
getcontext().rounding = ROUND_HALF_EVEN

pd.set_option("display.max_rows", 25)
pd.set_option("display.width", 1000)
pd.options.display.mpl_style = "default"

np.set_printoptions(linewidth=500)

RIPPLE_EPOCH = 946684800

# postgres connection
conn = db.connect("host=localhost dbname=grapple user=grapple")
conn.set_isolation_level(ext.ISOLATION_LEVEL_READ_COMMITTED)

class Grapple(object):

    def __init__(self, socket_url=None, setup=False, precision=28, halt=152370):
        self.setup = setup
        self.socket_url = socket_url
        self.start_date = None
        self.halt = halt
        self.socket = None
        self.ledger_current_index = None
        self.ledgers_to_read = None
        self.updates = 0
        self.currencies = self.get_currency_symbols()
        self.logger = logging.getLogger(__name__)
        getcontext().prec = precision

    def get_currency_symbols(self):
        with cursor() as cur:
            cur.execute("SELECT * FROM currencies")
            currencies = [row[0] for row in cur.fetchall()]
        return currencies

    def get_currency_pairs(self):
        for i, currency in enumerate(self.currencies):
            for other_currency in self.currencies[i+1:]:
                if currency + other_currency not in self.blacklist:
                    yield (currency, other_currency)

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
                    for i, curr in enumerate(self.currencies):
                        if curr == gets['currency'] and curr == pays['currency']:
                            break
                        if curr == gets['currency']:
                            gets_idx = i
                        if curr == pays['currency']:
                            pays_idx = i
                        if gets_idx is not None and pays_idx is not None:
                            break
                    if gets_idx < pays_idx:
                        temp = gets
                        gets = pays
                        pays = temp
                    if len(gets['currency']) > 3 or len(pays['currency']) > 3:
                        continue
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
                                    'historical': True,
                                    'accepted': accepted,
                                }
                                sql = """INSERT INTO ripple_ledger 
                                    (txid, txhash, market, currency1, currency2, amount1, amount2, 
                                    price1, price2, issuer1, issuer2, account1, 
                                    txdate, ledgerindex, historical, accepted, collected) 
                                    VALUES 
                                    (%(txid)s, %(txhash)s, %(market)s, %(currency1)s, %(currency2)s, 
                                    %(amount1)s, %(amount2)s, %(price1)s, %(price2)s, 
                                    %(issuer1)s, %(issuer2)s, %(account1)s, 
                                    %(txdate)s, %(ledgerindex)s, %(historical)s, %(accepted)s, now())"""
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
            vals = [Decimal(v).quantize(Decimal('.00000001'), 
                    rounding=ROUND_HALF_EVEN) for v in vals]
            row.extend(vals)
            row.extend([False])
            valuestr = ','.join(['%s'] * len(row))
            query = (
                "INSERT INTO resampled_ledger "
                "(starttime, freq, currency1, currency2, open1, high1, low1, "
                "close1, volume1, price1, open2, high2, low2, close2, "
                "volume2, price2, synthetic, data_source) "
                "VALUES (%s, 'wss://s1.ripple.com:51233')"
            ) % valuestr
            cur.execute(query, tuple(row))
            self.updates += 1

    def resample_time_series(self, drop):
        freqs = ('D', '8H', '4H', '2H', 'H', '30T', '15T')
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
                if drop:
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
        # Create index        
        conn.set_isolation_level(ext.ISOLATION_LEVEL_AUTOCOMMIT)
        with cursor() as cur:
            print "Indexing..."
            idx_queries = (
                "DROP INDEX IF EXISTS idx_ledger_interval",
                """CREATE INDEX CONCURRENTLY idx_ledger_interval ON 
                resampled_ledger(starttime, freq, currency1, currency2)"""
            )
            for query in idx_queries:
                cur.execute(query)

    def housekeeping(self):
        with cursor() as cur:
            query = (
                "DELETE FROM ripple_ledger "
                "WHERE ledgerindex IS NOT NULL AND historical = 't'"
            )
            cur.execute(query)
            print "Deleted", cur.rowcount, "transactions from database"

    def find_target_ledger(self):
        with cursor() as cur:
            cur.execute("SELECT max(ledgerindex) FROM ripple_ledger")
            for row in cur:
                max_ledgerindex = row[0]
            if max_ledgerindex is not None:
                self.halt = int(max_ledgerindex)

    def rippled_history(self, first=True):
            if self.rippled_connect():
                self.get_current_index()
                if not first:
                    print "\n***Fetching new ledgers***\n"
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
                                    if not first and self.ledger_index == self.halt:
                                        if self.is_duplicate(tx_hash):
                                            continue
                                    tx_data_result, options = self.get_tx(tx_hash, ledger)
                                    if tx_data_result:
                                        self.stored_tx += self.parse_tx(tx_data_result,
                                                                        accepted,
                                                                        **options)
                        if self.ledger_index % 1000 == 0:
                            self.clean_data_tables()
                        self.ledger_index -= 1
                    except Exception as exc:
                        print exc
                self.socket.close()
                print
                return True
            return False

    def clean_data_tables(self):
        with cursor() as cur:
            cur.execute("SELECT clean_data_tables()")

    def update_loop(self, interval=900):
        """Fetch data from rippled every (interval) seconds.

        Walk from the current ledger to the ledger index specified in the "halt"
        parameter, downloading transactions from rippled.
        
        """
        first = self.setup
        if first:
            self.housekeeping()
            self.rippled_history(first=True)
        while True:
            time_remaining = interval - time.time() % interval
            time.sleep(time_remaining)
            self.rippled_history(first=False)
            self.resample_time_series(first)
            self.clean_data_tables()
            first = False


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
        short_opts = 'hrsw:'
        long_opts = ['help', 'remote', 'setup', 'websocket=']
        opts, vals = getopt.getopt(argv[1:], short_opts, long_opts)
    except getopt.GetoptError as e:
        print >>sys.stderr, e.msg
        print >>sys.stderr, "for help use --help"
        return 2
    parameters = {
        'setup': True,
        'precision': 28,
        'socket_url': 'ws://127.0.0.1:6006/',
        # 'halt': 152370,  # Genesis ledger
        'halt': 8141600,
    }
    for opt, arg in opts:
        if opt in ('-h', '--help'):
            print __doc__
            return 0
        elif opt in ('-w', '--websocket'):
            parameters['socket_url'] = arg
        elif opt in ('-r', '--remote'):
            parameters['socket_url'] = 'wss://s1.ripple.com:51233/'
        elif opt in ('-s', '--setup'):
            parameters['setup'] = True
    grapple = Grapple(**parameters)
    grapple.update_loop()
    try:
        if conn:
            conn.close()
    except:
        pass

if __name__ == '__main__':
    sys.exit(main())
