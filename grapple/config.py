POSTGRES = {
    "user": "analytics",
    "pass": "",
    "dbname": "analytics",
    "host": "localhost",
    "port": "5432",
}
POSTGRES_CONNECTION_STRING = "host=%(host)s dbname=%(dbname)s user=%(user)s" % POSTGRES

RIPPLE_PUBLIC_WEBSOCKET = "wss://s1.ripple.com:51233/"
RIPPLE_EPOCH = 946684800
