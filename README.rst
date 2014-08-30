Grapple
=======

Download the Ripple ledger from rippled.

Grapple extracts the ledger from rippled via websocket.  It starts at the current ledger index, and walks backwards until it reaches the genesis ledger.  The genesis ledger index is set by default to 152370, but is adjustable.

Grapple can collect data from a local or remote rippled instance.  If you have a local rippled instance running that has downloaded all or most of the ledger, I strongly recommend doing local data collection.  Fetching data from Ripple's public websocket is very slow!

Also resamples the transaction time series to create "Open-Hi-Lo-Close" data, which can be useful for statistical tests, or simply for drawing charts.

Installation::

    pip install grapple

Grapple is designed to integrate with PostgreSQL, using connection information in config.py.  By default, it assumes that your database is located on localhost (127.0.0.1), and that your database username, password, and password are all "grapple".  (This is obviously not the most secure setup, but it may be convenient for people who install Grapple via pip, and do not wish to edit its source code.)

Usage as a Python module::

    from grapple import Grapple

    grapple = Grapple()
    grapple.download()

Usage as a script::

    python grapple.py [-flags]

Optional flags::

    -w, --websocket [websocket_url]:
        Specify the rippled websocket url. (default=ws://127.0.0.1:6006/)

    -p, --public:
        Use Ripple Labs' public websocket, wss://s1.ripple.com:51233.

    -f, --full:
        Resets the database tables.
