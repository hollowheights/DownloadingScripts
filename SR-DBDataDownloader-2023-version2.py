import polygon
import pandas as pd
import sqlalchemy
from sqlalchemy import Column, text, select, func
from sqlalchemy.pool import QueuePool
import datetime
import time
import pytz
from dateutil import tz
import concurrent.futures
import itertools
import logging
import os
import pickle
from API import api #custom module for importing the API key

# --- Explanatory notes ---
# The intraday table needs to be set up manually -> currently through 'SR-Postgresql-Reader.py'
# The daily table will at default be set up automatically through pd.to_sql() - but I used 'SR-Postgresql-Reader.py'

# --- Debugger/logging settings ---
logging.basicConfig(level=logging.CRITICAL) # possible values include: "DEBUG", "CRITICAL" - always with caps
# Disable the urllib debugger messages (they are usually unnecessary and take up a lot of space in the log)
logging.getLogger("urllib3").propagate = False
#logging.getLogger('sqlalchemy.engine').setLevel(logging.CRITICAL)  # --> avoid ultra long logs for queries

pd.set_option("display.max_rows", 300, "display.min_rows", 200, "display.max_columns", None, "display.width", None)

def unix_convert(ts):
    '''OBS: Her gemmer jeg i UTC tid for kompatibilitet med feather'''
    ts = int(ts/1000)
    tdate = datetime.datetime.utcfromtimestamp(ts).replace(tzinfo=tz.UTC) #.astimezone(tz.gettz('America/New_York'))
    return tdate

def unix_convert_date(ts):
    '''OBS: Her gemmer jeg i UTC tid for kompatibilitet med feather'''
    ts = int(ts/1000)
    tdate = datetime.datetime.utcfromtimestamp(ts).replace(tzinfo=tz.UTC) #.astimezone(tz.gettz('America/New_York'))
    tdate = tdate.date()
    return tdate

# --- Connect to Polygon ---
stocks_client = polygon.StocksClient(api)  # for OHLC data
ref_client = polygon.ReferenceClient(api)  # for reference data (MCAP etc.)


# --- Postgresql connection ---
# New in this version: only one database -> only one engine
try:
    dbpath = 'postgresql+psycopg2://postgres:postgres@localhost:5432/US_Listed'
    engine = sqlalchemy.create_engine(dbpath, pool_size=50, max_overflow=0)
    engine.connect()

except:
    logging.critical("Database not accessible currently")


# ---- Variable defaults
startdate_default = datetime.datetime(2018, 1, 1).date()    #--> default for daily data function
starttime_default = datetime.datetime(2018, 1, 1, 0, 0, 0)  # --> default for intraday function

EST_datetime_now = datetime.datetime.now(pytz.timezone('America/New_York')) #--> Define latest data to pull

# Check if it's too early to load new daily data - to avoid incomplete daily data (since it's not overwritten later)
# EST needs to be > 16:15 since Polygon has a 15min delay
if EST_datetime_now.hour > 16 or (EST_datetime_now.hour >= 16 and EST_datetime_now.minute > 15):
    enddate = EST_datetime_now.date()
else:
    enddate = EST_datetime_now.date() + datetime.timedelta(-1)

logging.debug(f"Value of enddate is: {enddate}")

# ---- Define ticker list---- (possibly a minor inaccuracy here, but should be correct - using the ticker list for the latest finished trading day)
# The list is updated on each run - IS IT ?

with open("tickerlist.pkl", "rb") as f:
    ticker_list = pickle.load(f)

# TO-DO:
# OBS CURRENTLY NOT IN USE
def ticker_list_fixer():
    ticker_list_cumulative = open("TickerList.txt").read().split("\n")

    ticker_resp = ref_client.get_tickers(symbol_type='CS', date=enddate, market='stocks', order='asc', active=True, all_pages=True)

    ticker_list_current = [x['ticker'] for x in ticker_resp]

    #https://stackoverflow.com/questions/1720421/how-do-i-concatenate-two-lists-in-python
    ticker_list = list(set(ticker_list_cumulative + ticker_list_current))

    print("Number of new tickers: ", (len(ticker_list) - len(ticker_list_cumulative)) )

    # TO_DO - file should be updated here? store to file...


# ---------------------- Function definitions ---------------------

def dataloader_daily(stock):
    startdate = startdate_default

    logging.debug(f"dataloader_daily now starting for ticker: {stock}")

    # Check for existing data for the ticker and get max date
    with engine.connect() as conn:
        try:
            max_date_raw = conn.execute(text("SELECT MAX(\"Date\") FROM daily WHERE \"Stock\" = :stock"), stock=stock)
            max_date_row = max_date_raw.fetchone()  # Fetch the first row as a tuple
            max_date = max_date_row[0] if max_date_row is not None else None  # Access the date value from the tuple
            max_date = str(max_date)

            if max_date is not None: # meaning there is existing data
                logging.debug(f"There is existing daily data for: {stock}")

                # Set 'startdate' to day after latest existing daily entry -> avoid overlapping data
                max_date_datetime = datetime.datetime.strptime(max_date, "%Y-%m-%d").date()
                startdate = max_date_datetime + datetime.timedelta(1)
                logging.debug(f"Most recent date: {max_date_datetime}, 'startdate' for new download: {startdate}")

            elif max_date is None:
                logging.debug(f"There is existing data but seems to be a problem with the data format (date)")

        except Exception as e:
            logging.debug(f"An error occured trying to establish existing data, for: {stock}, defaulting to startdate: {startdate}")
            logging.debug(e)

    logging.debug(f"Ready to download new daily data, value of startdate is: {startdate} and value of enddate is: {enddate}")

    # Download OHLC data - both adjusted and unadjusted
    daily_data_adj = stocks_client.get_aggregate_bars(stock, startdate, enddate, full_range=True, adjusted=True, warnings=False)
    daily_data_unadj = stocks_client.get_aggregate_bars(stock, startdate, enddate, full_range=True, adjusted=False, warnings=False)

    # Check if new data is returned, pass if none
    if len(daily_data_adj) > 0:

        # Set up dataframes
        df_daily_data_adj = pd.DataFrame(daily_data_adj)
        df_daily_data_unadj = pd.DataFrame(daily_data_unadj)

        logging.debug(f"New data loaded: {df_daily_data_adj.head()}")

        # Call next function
        datamanipulator_daily(stock, df_daily_data_adj, df_daily_data_unadj)
    else:
        # no need for logging here - datastorer_daily does this
        datastorer_daily(stock)

def datamanipulator_daily(stock, df_daily_data_adj, df_daily_data_unadj):

    # Merge data
    df_daily = df_daily_data_adj

    # Convert timestamp to UTC time - again: using UTC > EST for now
    df_daily['Date'] = df_daily['t'].map(lambda x: unix_convert_date(x))

    # Manually specify that it's a datetime (necessary!)
    df_daily['Date'] = pd.to_datetime(df_daily['Date'])

    # Add 4 columns of unadjusted data
    df_daily[['OpenUnadjusted', 'HighUnadjusted', 'LowUnadjusted', 'CloseUnadjusted']] = df_daily_data_unadj[['o', 'h', 'l', 'c']]

    # delete unwanted columns
    df_daily.drop(["vw", "t"], axis=1, inplace=True)

    # Add 'Stock' column
    df_daily['Stock'] = stock

    # Define list of all trading days to pull reference data for
    list_trading_dates = df_daily['Date'].tolist() # version to test if datetimes can be passed directly -> dict_MCAP matches on datatype

    # Add MCAP data to the dataframe -> calling dedicated function
    try:
        MCAP_data = dataloader_MCAP(stock, list_trading_dates)
        #df_daily['MarketCap'] = df_daily['Date'].map(MCAP_data)
        df_daily['MarketCap'] = df_daily['Date'].map(lambda x: MCAP_data.get(x))
    except Exception as e:
        logging.CRITICAL("Exception for market cap call")
        logging.CRITICAL(e)

    # reorder columns
    df_daily = df_daily[['Date', 'Stock', 'o', 'h', 'l', 'c', 'OpenUnadjusted', 'HighUnadjusted',
                         'LowUnadjusted', 'CloseUnadjusted', 'MarketCap', 'v', 'n']]

    # rename columns - trying explicit redefinition instead of 'in_place=True'
    df_daily = df_daily.rename(columns={'o':'Open', 'h':'High', 'l':'Low', 'c':'Close', 'v':'Volume'})

    logging.debug(f"Test print of the data:")
    logging.debug(df_daily.tail(20))

    # -- Add columns based on existing data ----
    df_daily["$Volume"] = df_daily["Volume"] * ((df_daily["Close"] + df_daily['Open']) / 2)
    df_daily["AvgVolume10D"] = df_daily["Volume"].rolling(10).mean()
    df_daily["Range"] = (abs(df_daily['High'] - df_daily['Low'])/df_daily['Open']) * 100
    df_daily['PrevDayClose'] = df_daily.CloseUnadjusted.shift(1)
    df_daily['Weekday'] = df_daily.Date.dt.dayofweek

    logging.debug(f"Test print of the data - after adding more columns:")
    logging.debug(df_daily.tail(20))


    datastorer_daily(stock, df_daily)

def dataloader_MCAP_perday(stock, day):
    current_data = ref_client.get_ticker_details(symbol=stock, date=day, raw_response=True)
    current_data = current_data.json()

    # TO-DO: possibly add a check here for failed requests

    return day, current_data['results']['market_cap'] # Return a tuple with the input day and the response

def dataloader_MCAP(stock, list_trading_dates):
    with concurrent.futures.ThreadPoolExecutor(2000) as executor:
        try:
            #executor.map(dataloader_MCAP_perday, itertools.repeat(stock), list_trading_dates)
            futures = [executor.submit(dataloader_MCAP_perday, stock, date) for date in list_trading_dates]

            results = {}

            # Wait for all futures to complete
            concurrent.futures.wait(futures)

            for future in futures:
                if future.done() and future.exception() is None:
                    results[future.result()[0]] = future.result()[1]
                else:
                    logging.debug(f"There was an incomplete future for ticker: {stock}")

        except Exception as e:
            logging.CRITICAL(e)

    return results


def dataloader_intraday(stock):
    #currently uses the copy from csv method only for cases with no existing data -> optimizing load times into postgres
    #possibly simpler to use this method for all cases - requires a reliable 'if exists append' equivalent

    starttime = starttime_default
    logging.debug(f"Starting dataloader_intraday for: {stock}")
    with engine.connect() as conn:
        # Check for existing data - if so: get max date
        max_time_raw = conn.execute(text("SELECT MAX(\"Time\") FROM intraday WHERE \"Stock\" = :stock"), stock=stock)
        max_time = max_time_raw.fetchone()[0]

    if max_time is None:
        logging.debug(f"No existing intraday data for ticker: {stock}, 'starttime' will default to {starttime}")
    else:
        starttime = max_time + datetime.timedelta(minutes=1)
        logging.debug(f"There is existing intraday data for {stock}. Most recent timestamp is: {max_time}")

    # Load in new data
    intraday_data = stocks_client.get_aggregate_bars(stock, starttime, EST_datetime_now, timespan='minute', multiplier=1,
                                                     full_range=True, adjusted=False, warnings=False)

    # Check if new data is returned, pass if none
    if len(intraday_data) > 0:

        # Set up dataframe
        df_intraday = pd.DataFrame(intraday_data)

        # Call next function - the intraday manipulator function
        datamanipulator_intraday(stock, df_intraday)

    # If no new data: calling storage function without passing new data
    else:
        datastorer_intraday(stock)
        logging.debug(f"There was no new intraday data downloaded for ticker: {stock}")

def datamanipulator_intraday(stock, df_intraday):

    # Convert timestamp to UTC time - not using EST for now due to Postgres + compatibility with other scripts
    df_intraday['t'] = df_intraday['t'].map(lambda x: unix_convert(x))

    # Manually specify that it's a datetime (necessary!)
    df_intraday['Time'] = pd.to_datetime(df_intraday['t'])

    # reorder columns - indirectly deleting some here [does not prompt copy vs slice warning]
    df_intraday = df_intraday[['Time', 'o', 'h', 'l', 'c', 'v', 'n']]

    # Add the 'stock' column
    df_intraday.insert(0, 'Stock', stock)

    # rename columns
    df_intraday.columns = ['Stock', 'Time', 'Open', 'High', 'Low', 'Close', 'Volume', 'n']

    # Storage to postgres - check which method to use
    if len(df_intraday) < 2_000:
        logging.debug(f"New data < 2000 rows -> now storing data using 'pd.to_sql()' ")
        datastorer_intraday(stock, df_intraday)
    else:
        logging.debug(f"New data will be stored via 'csv_postgres_loader' ")
        csv_postgres_loader(stock, df_intraday)


def csv_postgres_loader(stock, df):
    # Sequence: 1) store received dataframe to csv, 2) create postgres table (check if exists), 3) load data into postgres
    # 4) delete csv file from disk

    #store received dataframe to csv
    df.to_csv('%s_temp.csv' % stock, index=False, mode='w')

    #Load csv into postgres
    with open('%s_temp.csv' % stock) as file:
        conn = engine.raw_connection()
        cursor = conn.cursor()
        try:
            cursor.copy_expert('COPY "intraday" FROM STDIN WITH CSV HEADER', file)
        except Exception as e:
            logging.critical(f"Error writing to intraday database for ticker: {stock}")
            logging.critical(str(e))
        conn.commit()
        cursor.close()
        conn.close()

    # Delete the csv file
    os.remove('%s_temp.csv' % stock)


def datastorer_daily(stock, df_daily=None):

    if df_daily is None: #will default to none and 'else' will execute when anything is passed to the function
        print(f"There was no new daily data stored for: {stock}")

    else:
        if len(df_daily) > 0:
            try:
                # Store daily data
                pd.DataFrame.to_sql(df_daily, "daily", con=engine, if_exists='append', index=False, method='multi', chunksize=10000)

                number_new_rows = len(df_daily)
                print(f"{number_new_rows} new days stored for {stock}, latest date is: {df_daily.Date.max()}")
            except Exception as e:
                logging.debug(e)

        #following section should not be necessary
        else:
            print(f"There was no new daily data stored for {stock}")
            logging.debug(f"An empty 'df_daily' was passed")


def datastorer_intraday(stock, df_intraday = None):
    if df_intraday is None:
        logging.debug(f"No new intraday data")

    else:
        logging.debug(f"There is new intraday data for {stock}, {len(df_intraday)} new rows")
        logging.debug(f"Most recent row of the new data is: {df_intraday.Time.max()}")

        # store intraday data
        pd.DataFrame.to_sql(df_intraday, 'intraday', con=engine, if_exists='append', index=False, chunksize=10000, method='multi')
        logging.debug(f'Latest intraday data added: \n {df_intraday.tail(1)}')


#----------------- Run functions -----------------------

# Redefine ticker_list for testing purposes
# ticker_list = ["F", "SNOA", "TSLA"]
#ticker_list = ["SNOA","F", "ITOS", "PMN", "CRKN"]

# Define start time for the program
starttime_perf = time.perf_counter()

def main(ticker):
    # Define start time for per ticker measure
    ticker_start_time = time.perf_counter()
    dataloader_daily(ticker)
    logging.debug("Finished running dataloader_daily, now starting dataloader_intraday")

    dataloader_intraday(ticker)
    ticker_end_time = time.perf_counter()
    #new_rows_total =
    #print('Finished storing {new_rows_total} rows for %s. Total time: %s ' % (ticker, ticker_end_time - ticker_start_time))

with concurrent.futures.ThreadPoolExecutor(1) as executor:
    executor.map(main, ticker_list)

endtime_perf = time.perf_counter()
print("Total program execution time: ", endtime_perf - starttime_perf)


'''
Atlernative method for checking for existing daily data (from GPT)
#query = text("SELECT EXISTS (SELECT 1 FROM daily WHERE \"Stock\" = :stock)")
#check_value = conn.execute(query, stock=stock).scalar()
# Convert the result to a boolean value
#exists_stock = bool(check_value)


#checkvalue = conn.execute(text("SELECT COUNT(*) FROM daily WHERE \"Stock\" = :stock"), stock=stock)
#count = checkvalue.fetchone()[0]

df_testio = pd.DataFrame.from_dict(MCAP_data, orient='index', columns=['MarketCap'])
df_testio = df_testio.reset_index()
df_daily['MarketCap'] = df_testio['MarketCap']


df_daily.columns = ['Date', 'Stock', 'Open', 'High', 'Low', 'Close',
                    'OpenUnadjusted', 'HighUnadjusted', 'LowUnadjusted', 'CloseUnadjusted', 'MarketCap',
                    'Volume', 'n']


with open("TickerList.txt", 'r') as file:
    text = file.readlines()

    mylist = list(text)
    print(mylist[0:10])

    print(len(mylist))


with open('TickerList.txt') as f:
    ticker_list = f.readlines()
    ticker_list = list(ticker_list)
    ticker_list = ticker_list[0:3]
    print(len(ticker_list))

print(ticker_list[0])
for x in ticker_list[0:3]:
    print("1")


#conn = psycopg2.connect(host='localhost', dbname='mydb', user='postgres', password='postgres', port=5432)
#cur = conn.cursor()


n = df_daily.shape[0] - len(mcap_list)  # Find the difference in row number
    df_daily.drop(df_daily.tail(n).index, inplace=True) # Delete last n rows to match data
    df_daily['MarketCap'] = mcap_list


  # Add MCAP data to the dataframe - METHOD 1
    mcap_list = [ref_data_raw[x]['results']['market_cap'] for x in ref_data_raw]
    df_daily['MarketCap'] = mcap_list
    
    for day in list_trading_dates:
        current_data = ref_client.get_ticker_details(symbol=stock, date=day)
        try:
            dict_MCAP[day] = current_data['results']['market_cap']
        except:
            dict_MCAP[day] = np.nan

    #startdate = date - datetime.timedelta(20)  # offset med x antal dage -> skulle ideelt være x børsdage


def datadownloader(stock):
    pass
    # // daily sub function
    #if table exists in daily db:
        #define max date in current data
        #download latest data
        #modify data to my format
        # add to database
    #else:
        #download all data since 2018-01-01
        # modify data to my format
        # create table

    # // intraday sub function
    #if table exists in intraday db:
        #define max datetime in current data
        #download latest data
        #modify (if needed)
        #add to database
    #else:
        #download all data since 2018-01-01
        # modify (if needed)
        # add to database

    #list_trading_dates = df_daily.Date.values.tolist() # THIS METHOD DOES NOT PRESERVE THE FORMAT/DATA TYPE!!!
    #list_trading_dates = df_daily['Date'].dt.strftime('%Y-%m-%d').tolist() #CHAT GPT ALTERNATIVE SOLUTION
'''