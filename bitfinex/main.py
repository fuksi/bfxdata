import json
import logging
import time
import click
import pendulum

from db import SqliteDatabase
from utils import date_range, get_data

logger = logging.getLogger()
handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s %(levelname)-8s %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.INFO)

API_URL = 'https://api.bitfinex.com/v2'


def symbol_start_date(symbol):
    return pendulum.parse('2017-01-01T00:00:00Z')
    
def get_symbols():
    with open('symbols.json') as f:
        return json.load(f)

def get_f_symbols():
    with open('symbols_funding.json') as f:
        return json.load(f)

def get_t_symbols():
    with open('symbols_trading.json') as f:
        return json.load(f)

def get_candles(symbol, start_date, end_date, timeframe='1m', limit=1000):
    """
    Return symbol candles between two dates.
    https://docs.bitfinex.com/v2/reference#rest-public-candles
    """
    # timestamps need to include milliseconds
    start_date = start_date.int_timestamp * 1000
    end_date = end_date.int_timestamp * 1000

    url = f'{API_URL}/candles/trade:{timeframe}:t{symbol.upper()}/hist' \
          f'?start={start_date}&end={end_date}&limit={limit}'
    data = get_data(url)
    return data

def get_trades(symbol, start_date, limit=1000):
    start_date = start_date.int_timestamp * 1000

    # go backward
    url = f'{API_URL}/trades/t{symbol.upper()}/hist' \
          f'?start={start_date}&limit={limit}&sort=1'

    data = get_data(url)
    return data

def get_funding_trades(symbol, start_date, limit=1000):
    start_date = start_date.int_timestamp * 1000

    # go backward
    url = f'{API_URL}/trades/f{symbol.upper()}/hist' \
          f'?start={start_date}&limit={limit}&sort=1'

    data = get_data(url)
    return data


@click.command()
@click.option('--debug', is_flag=True, help='Set debug mode')
@click.option('--usemssql', is_flag=True, help='Use mssql instead of postgres')
@click.option('--includecandles', is_flag=True, help='Get candles')
@click.option('--includefundings', is_flag=True, help='Get fundings')
@click.option('--includetradings', is_flag=True, help='Get tradings')
@click.option('--pghost')
@click.option('--pgdb')
@click.option('--pguser')
@click.option('--pgpw')
def main(debug, usemssql, includecandles, includefundings, includetradings, pghost, pgdb, pguser, pgpw):

    debug, includetradings = True, True
    usemssql, includecandles, includefundings = False, False, False

    if debug:
        logger.setLevel(logging.DEBUG)

    if usemssql:
        raise ValueError("MSSQL not supported") 
    else:
        db = SqliteDatabase(pghost, pgdb, pguser, pgpw)
        print('Using postgres adapter')

    end_date = pendulum.now()
    step = pendulum.Duration(minutes=1000)

    symbols = get_symbols()
    logging.info(f'Found {len(symbols)} trading symbols')
    f_symbols = get_f_symbols()
    logging.info(f'Found {len(f_symbols)} funding symbols')
    t_symbols = get_t_symbols()
    logging.info(f'Found {len(t_symbols)} trading symbols')

    while True:
        end_date = pendulum.now()
        if includecandles:
            for i, symbol in enumerate(symbols, 1):
                # get start date for symbol
                # this is either the last entry from the db
                # or the trading start date (from json file)
                latest_candle_date = db.get_latest_candle_date(symbol)
                if latest_candle_date is None:
                    logging.debug('No previous entries in db. Starting from scratch')
                    # TODO: handle case when symbol is missing from trading start days
                    # e.g. symbol is in symbols.json but not in symbols_trading_start_days.json
                    start_date = symbol_start_date(symbol)
                else:
                    logging.debug('Found previous db entries. Resuming from latest')
                    start_date = latest_candle_date

                logging.info(f'{i}/{len(symbols)} | {symbol} | Processing from {start_date.to_datetime_string()}')
                for d1, d2 in date_range(start_date, end_date, step):
                    logging.debug(f'{d1} -> {d2}')
                    # returns (max) 1000 candles, one for every minute
                    candles = get_candles(symbol, d1, d2)
                    logging.debug(f'Fetched {len(candles)} candles')
                    if candles:
                        db.insert_candles(symbol, candles)

                    # prevent from api rate-limiting
                    time.sleep(3)

        if includefundings:
            for i, f_symbol in enumerate(f_symbols, 1):
                latest_funding_date = db.get_latest_funding_date(f_symbol)
                if latest_funding_date is None:
                    logging.debug('No previous entries in db. Starting from scratch')
                    start_date = symbol_start_date(f_symbol)
                else:
                    logging.debug('Found previous db entries. Resuming from latest')
                    start_date = latest_funding_date

                logging.info(f'{i}/{len(f_symbols)} | {f_symbol} | Processing from {start_date.to_datetime_string()} ')
                prev_start_date = start_date.subtract(days=1)
                while start_date < end_date and prev_start_date.diff(start_date).in_seconds() > 60:
                    logging.debug(f'Fetching trades from {start_date} ...')
                    f_trades = get_funding_trades(f_symbol, start_date)
                    logging.debug(f'Fetched {len(f_trades)} candles')

                    prev_start_date = start_date
                    if f_trades:
                        db.insert_funding_trades(f_symbol, f_trades)
                        start_date = pendulum.from_timestamp(f_trades[-1][1]/1000)
                    else:
                        start_date = start_date.add(minutes=10)

                    time.sleep(3)

        if includetradings:
            for i, t_symbol in enumerate(t_symbols, 1):
                latest_trading_date = db.get_latest_trading_date(t_symbol)
                if latest_trading_date is None:
                    logging.debug('No previous entries in db. Starting from scratch')
                    start_date = symbol_start_date(t_symbol)
                else:
                    logging.debug('Found previous db entries. Resuming from latest')
                    start_date = latest_trading_date
                logging.info(f'{i}/{len(t_symbols)} | {t_symbol} | Processing from {start_date.to_datetime_string()} ')
                
                while start_date < end_date:
                    logging.debug(f'Fetching trades from {start_date} ...')
                    trades = get_trades(t_symbol, start_date)
                    logging.debug(f'Fetched {len(trades)} trades')

                    if trades:
                        db.insert_trades(t_symbol, trades)
                        start_date = pendulum.from_timestamp(trades[-1][2]/1000)

                        # VERY EDGE CASE HERE
                        # if there is > 1000 trades during 1s, we can't get other trades than the first 1000 one
                        # because bfx use second based (not ms) timestamp as a search param, and limit is 1000 records
                        # We'll stuck in infinite loop if our next query timestamp does not increase at least by 1s 
                        # not much we can't do, but to continue with the next second
                        # since API limit is 1000 per request for a particular timestamp
                        if len(trades) > 1 and abs(trades[-1][2] - trades[0][2]) < 1000:
                            start_date = start_date.add(seconds=1)
                    else:
                        start_date = start_date.add(seconds=10)

                    time.sleep(3)
        
        logger.info('Went through all symbols. Start over again!')

if __name__ == '__main__':
    main()
