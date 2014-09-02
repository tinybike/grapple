#!/usr/bin/env python
"""Grapple unit tests.

Remove the leading underscore from _test_init_fullrun and _test_init_smallrun
to do download tests, if your machine has a working rippled and Postgres
installation.

The other tests are run by connecting to Ripple Labs' public websocket.
At the moment, there are no tests for the methods that require a
database connection.

"""
from __future__ import division, print_function, unicode_literals, absolute_import
try:
    import sys
    import cdecimal
    sys.modules["decimal"] = cdecimal
except:
    pass
import os
import unittest
from decimal import Decimal, getcontext, ROUND_HALF_EVEN
import psycopg2 as db
import psycopg2.extensions as ext

HERE = os.path.dirname(os.path.realpath(__file__))
sys.path.insert(0, os.path.join(HERE, os.pardir, "grapple"))

from grapple import Grapple

class TestGrapple(unittest.TestCase):

    def setUp(self):
        self.txhash = "BC02D079CB7B2087C70F857E6EDAFD72229887B9313C776890FB92D59CF3DD54"
        self.grapple = Grapple(socket_url="wss://s1.ripple.com:51233/")
        self.assertIsNone(self.grapple.socket)        

    def _test_init_fullrun(self):
        self.grapple = Grapple(full=True)
        self.assertTrue(self.grapple.full)
        self.grapple.download()
        
    def _test_init_smallrun(self):
        self.grapple = Grapple(genesis=8642812)
        self.assertEqual(self.grapple.halt, 8642812)
        self.grapple.download()

    def test_init_resampling_frequencies(self):
        self.grapple = Grapple(resampling_frequencies=('8T', '12T'))
        self.assertEqual(self.grapple.resampling_frequencies, ('8T', '12T'))

    def test_rippled_connect(self):
        self.assertIsNone(self.grapple.socket)
        self.grapple.rippled_connect()
        self.assertIsNotNone(self.grapple.socket)
        self.assertTrue(self.grapple.socket.connected)

    def test_get_current_index(self):
        self.grapple.rippled_connect()
        self.grapple.get_current_index()
        self.assertIsNotNone(self.grapple.ledger_current_index)
        self.assertEqual(type(self.grapple.ledger_current_index), int)
        self.assertGreater(self.grapple.ledger_current_index, 8642812)

    def test_read_next_ledger(self):
        self.grapple.rippled_connect()
        self.grapple.get_current_index()
        self.grapple.ledgers_to_read = self.grapple.ledger_current_index - self.grapple.halt
        self.grapple.ledger_index = self.grapple.ledger_current_index - 1
        self.grapple.stored_tx = 0
        ledger = self.grapple.read_next_ledger()
        self.assertIsNotNone(ledger)

    def test_parse_ledger(self):
        self.grapple.rippled_connect()
        self.grapple.get_current_index()
        self.grapple.ledgers_to_read = self.grapple.ledger_current_index - self.grapple.halt
        self.grapple.ledger_index = self.grapple.ledger_current_index - 1
        self.grapple.stored_tx = 0
        ledger = self.grapple.read_next_ledger()
        tx_hash_list, accepted = self.grapple.parse_ledger(ledger)

    def test_get_tx(self):
        self.grapple.rippled_connect()
        self.grapple.get_current_index()
        self.grapple.ledgers_to_read = self.grapple.ledger_current_index - self.grapple.halt
        self.grapple.ledger_index = self.grapple.ledger_current_index - 1
        self.grapple.stored_tx = 0
        ledger = self.grapple.read_next_ledger()
        tx_hash_list, accepted = self.grapple.parse_ledger(ledger)
        tx_data_result, options = self.grapple.get_tx(self.txhash, ledger)
        self.assertIsNotNone(tx_data_result)

    def tearDown(self):
        if self.grapple.socket and self.grapple.socket.connected:
            self.grapple.socket.close()
        del self.grapple


if __name__ == "__main__":
    suite = unittest.TestLoader().loadTestsFromTestCase(TestGrapple)
    unittest.TextTestRunner(verbosity=2).run(suite)
