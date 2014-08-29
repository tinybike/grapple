## Grapple

Grapple is a tool for downloading the Ripple ledger.  It extracts the ledger from rippled via websocket, by starting at the most current ledger index, and walking backwards until it gets to the genesis ledger (ledger 152370).

Grapple can collect data from a local or remote (e.g., `wss://s1.ripple.com:51233`) rippled instance.  If you have a local rippled instance running that has downloaded all or most of the ledger, I strongly recommend doing local data collection, as fetching data from Ripple's public websocket takes a *very* long time.
