DROP TABLE IF EXISTS resampled_ledger CASCADE;
CREATE TABLE resampled_ledger (
    starttime bigint,
    freq varchar(10),
    currency1 varchar(10),
    currency2 varchar(10),
    open1 numeric(24,8),
    open2 numeric(24,8),
    high1 numeric(24,8),
    high2 numeric(24,8),
    low1 numeric(24,8),
    low2 numeric(24,8),
    close1 numeric(24,8),
    close2 numeric(24,8),
    volume1 numeric(24,8),
    volume2 numeric(24,8),
    medprice1 numeric(24,8),
    synthetic boolean,
    data_source varchar(1000)
);

DROP TABLE IF EXISTS currencies CASCADE;
CREATE TABLE currencies (
    ticker varchar(10) NOT NULL PRIMARY KEY,
    name varchar(100),
    data_source varchar(1000)
);

DROP TABLE IF EXISTS ripple_ledger CASCADE;
CREATE TABLE ripple_ledger (
    internalid bigserial NOT NULL PRIMARY KEY,
    txid bigint,
    txhash varchar(1000),
    market varchar(20),
    currency1 varchar(10),
    currency2 varchar(10),
    price1 numeric(24,8),
    price2 numeric(24,8),
    amount1 numeric(24,8),
    amount2 numeric(24,8),
    issuer1 varchar(1000),
    issuer2 varchar(1000),
    account1 varchar(1000),
    account2 varchar(1000),
    txdate bigint,
    ledgerindex bigint,
    historical boolean,
    accepted boolean,
    collected timestamp DEFAULT statement_timestamp()
);
