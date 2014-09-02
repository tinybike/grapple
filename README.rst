Grapple
=======

.. image:: https://travis-ci.org/tensorjack/grapple.svg?branch=master
    :target: https://travis-ci.org/tensorjack/grapple

.. image:: https://coveralls.io/repos/tensorjack/grapple/badge.png
  :target: https://coveralls.io/r/tensorjack/grapple

Grapple extracts the full Ripple ledger directly from rippled via websocket.  It queries rippled for the current ledger index, then walks backwards until it reaches the genesis ledger, recording every transaction it finds.

Data can be collected from a local or a remote rippled instance.  If you have a local rippled instance running that has downloaded all or most of the ledger, I strongly recommend doing local data collection.  Fetching data from Ripple's public websocket is very slow!

Grapple also resamples each currency pair's price time series to create "Open-Hi-Lo-Close" time series.  These resampled datasets can be useful for statistical tests, technical market analysis, or simply for drawing charts.

Installation
^^^^^^^^^^^^

The easiest way to install Grapple is using pip::

    pip install grapple

Grapple is designed to integrate with PostgreSQL, using connection information in config.py.  By default, it assumes that your database is located on localhost (127.0.0.1), and that your database's name, password, username and are all "grapple".

(While this is certainly not a secure setup, it may be convenient for people who install Grapple via pip, and do not wish to edit its source code.)

Usage
^^^^^

Grapple can be used as a Python module::

    from grapple import Grapple

    grapple = Grapple()
    grapple.download()

The Grapple constructor accepts the following keyword arguments:

    socket_url (str):
        rippled websocket URL (default="ws://127.0.0.1:6006/")

    full (bool):
        True if downloading the full ledger (starting from the current ledger
        and walking back to the genesis ledger). False if the download should
        stop at the last current ledger (i.e., the last time grapple was run).
        (default=True)

    genesis (int):
        Genesis block index and download halting point. (default=152370)
    
    quiet (bool):
        If True, suppress console output. (default=True)
    
    resampling_frequencies (tuple):
        Resampling frequencies, using pandas frequency codes.  If None, then
        resampling is disabled. (default=('D',) or daily)

It can also be run as a script::

    python grapple.py [-flags]

Optional flags::

    -w, --websocket [websocket url]:
        Specify the rippled websocket url. (default=ws://127.0.0.1:6006/)

    -p, --public:
        Use Ripple Labs' public websocket, wss://s1.ripple.com:51233.

    -f, --full:
        Download the full Ripple ledger.  Automatic on your first run.

    -g, --genesis [ledger index]:
        Genesis ledger index and download halting point.

    -q, --quiet:
        Suppress command line output.

Tests
^^^^^

Unit tests are in the test/ directory.  Coverage is limited at the moment, but slowly improving!

Note: tests that require a local rippled and/or Postgres database connection are disabled by default.  See test/test_grapple.py for details.
