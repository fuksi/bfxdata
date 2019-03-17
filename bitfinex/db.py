import sqlite3
import pendulum
import psycopg2

class SqliteDatabase:
    def __init__(self, host, db, user, pw):
        self.host = host
        self.db = db
        self.user = user
        self.pw = pw
        self.create()

    def connect(self):
        return psycopg2.connect(host=self.host,database=self.db, user=self.user, password=self.pw)

    def create(self):
        with self.connect() as con:
            with con.cursor() as cur:
                cur.execute("""
                    create table if not exists candles (
                        symbol varchar(10) NOT NULL,
                        time timestamptz NOT NULL,
                        open decimal(18,10) NOT NULL,
                        close decimal(18,10) NOT NULL,
                        high decimal(18,10) NOT NULL,
                        low decimal(18,10) NOT NULL,
                        volume decimal(18,10) NOT NULL,
                        PRIMARY KEY (symbol, time)
                    );

                    create index if not exists candle_symbol_idx on candles (symbol);
                    create index if not exists candle_time_idx on candles (time);
                """)

                cur.execute("""
                    create table if not exists fundings (
                        symbol varchar(10) NOT NULL,
                        id int NOT NULL,
                        time timestamptz NOT NULL,
                        amount decimal(18,10) NOT NULL,
                        rate decimal(18,10) NOT NULL,
                        period smallint NOT NULL,
                        PRIMARY KEY (symbol, time)
                    )
                """)

                cur.execute("""
                    create table if not exists tradings (
                        symbol varchar(10) NOT NULL,
                        id int NOT NULL PRIMARY KEY,
                        time timestamptz NOT NULL,
                        amount decimal(18,10) NOT NULL,
                        price decimal(18,10) NOT NULL
                    );

                    create index if not exists symbol_idx on tradings (symbol);
                    create index if not exists time_idx on tradings (time);
                    create index if not exists symbol_time_idx on tradings (symbol, time);
                """)

        con.close()

    def insert_candles(self, symbol, candles):
        for candle in candles:
            candle.insert(0, symbol)

        with self.connect() as con:
            with con.cursor() as cur:
                args = [cur.mogrify('(%s, TO_TIMESTAMP(%s/1000), %s, %s, %s, %s, %s)', x).decode('utf-8') for x in candles]
                args_str = ','.join(args)
                cur.execute("""
                    insert into candles(
                        symbol, time, open, close, high, low, volume)
                    values """ + args_str + "on conflict do nothing")
                
        con.close()

    def insert_trades(self, symbol, trades):
        for trade in trades:
            trade.insert(0, symbol)

        with self.connect() as con:
            with con.cursor() as cur:
                args = [cur.mogrify('(%s, %s, TO_TIMESTAMP(%s/1000), %s, %s)', x).decode('utf-8') for x in trades]
                args_str = ','.join(args)
                cur.execute("""
                    insert into tradings(
                        symbol, id, time, amount, price)
                    values""" + args_str + "on conflict do nothing")
                
        con.close()

    def insert_funding_trades(self, symbol, trades):
        for trade in trades:
            trade.insert(0, symbol)

        with self.connect() as con:
            with con.cursor() as cur:
                args = [cur.mogrify('(%s, %s, TO_TIMESTAMP(%s/1000), %s, %s, %s)', x).decode('utf-8') for x in trades]
                args_str = ','.join(args)
                cur.execute("""
                    insert into fundings(
                        symbol, id, time, amount, rate, period)
                    values""" + args_str + "on conflict do nothing")
                
        con.close()

    def get_latest_candle_date(self, symbol):
        """
        Get the time of the most recent candle for a symbol
        """
        with self.connect() as con:
            with con.cursor() as cur:
                cur.execute('select max(time) from candles where symbol=%s',
                                    (symbol,))
                result = cur.fetchone()[0]
                if result is None:
                    return
                else:
                    return pendulum.instance(result)
                
        con.close()

    def get_latest_trading_date(self, symbol):
        """
        Get the time of the most recent trading for a symbol
        """
        with self.connect() as con:
            with con.cursor() as cur:
                cur.execute('select max(time) from tradings where symbol=%s',
                                    (symbol,))
                result = cur.fetchone()[0]
                if result is None:
                    return
                else:
                    return pendulum.instance(result)
                
        con.close()

    def get_latest_funding_date(self, symbol):
        """
        Get the time of the most recent funding for a symbol
        """
        with self.connect() as con:
            with con.cursor() as cur:
                cur.execute('select max(time) from fundings where symbol=%s',
                                    (symbol,))
                result = cur.fetchone()[0]
                if result is None:
                    return
                else:
                    return pendulum.instance(result)
                
        con.close()
