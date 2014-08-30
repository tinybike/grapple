POSTGRES = {
    "user": "grapple",
    "password": "grapple",
    "dbname": "grapple",
    "host": "localhost",
}

POSTGRES_CONNECTION_STRING = (
    "host=%(host)s dbname=%(dbname)s user=%(user)s password=%(password)s"
    % POSTGRES
)

RIPPLE_PUBLIC_WEBSOCKET = "wss://s1.ripple.com:51233/"
RIPPLE_EPOCH = 946684800
