Grapple
=======

Download the Ripple ledger directly from rippled.

Grapple extracts the ledger from rippled via websocket.  It starts at the current ledger index, and walks backwards until it reaches the genesis ledger.  The genesis ledger index is set by default to 152370, but is adjustable.

Grapple can collect data from a local or remote rippled instance.  If you have a local rippled instance running that has downloaded all or most of the ledger, I strongly recommend doing local data collection.  Fetching data from Ripple's public websocket is very slow!

Installation::

    pip install grapple    

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
