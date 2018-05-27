import requests
from bs4 import BeautifulSoup
from io import BytesIO
import numpy as np
import pandas as pd
import re
from datetime import datetime
from datetime import date
from datetime import timedelta
from datetime import timezone
import calendar
import MySQLdb
from sqlalchemy import create_engine


def krx_marketdata_download(date_str=None):  # Download market data for all KRX listed stocks
    # If parameter is left empty, assume today's date
    if date_str is None:
        date_str = datetime.today().strftime('%Y%m%d')

    # Generate OTP from KRX Marketdata
    gen_otp_url = "http://marketdata.krx.co.kr/contents/COM/GenerateOTP.jspx"
    gen_otp_data = {
        "name": "fileDown",
        "filetype": "xls",
        "url": "MKD/04/0404/04040200/mkd04040200_01",
        "market_gubun": "ALL",
        "indx_ind_cd": "",
        "sect_tp_cd": "",
        "schdate": date_str,
        "pagePath": "/contents/MKD/04/0404/04040200/MKD04040200.jsp"
    }

    r = requests.post(gen_otp_url, gen_otp_data)
    code = r.content

    # Download market data
    down_url = "http://file.krx.co.kr/download.jspx"
    down_data = {
        "code": code
    }

    r = requests.post(down_url, down_data)
    df = pd.read_excel(BytesIO(r.content), header=0, thousands=',', converters={'종목코드': str})
    return df


def naverfinance_financials_consensus(ticker, period, stmnt_type):
    url = "http://companyinfo.stock.naver.com/v1/company/ajax/cF1001.aspx"    # URL for financial estimate consensus in Naver Finance
    code = {    # Code to request specifics on the consensus
        "cmp_cd": ticker,
        "fin_typ": stmnt_type,
        "freq_typ": period
    }
    page = requests.get(url, code)    # Sending in request for page (URL and code)
    content = page.content    # Separating the content of the requested page
    soup = BeautifulSoup(content, "lxml")    # Parsing the content (html) into lxml format
    soup_clean = soup.prettify()    # Gets rid of tags used for formatting and spacing
    df_consensus = pd.read_html(soup_clean)[0]    # html table to be put into pandas dataframe format

    # Code to clean up the columns into pure dates
    length = len(df_consensus.columns)
    formatted_column_list = [df_consensus.columns[x][1] for x in range(0, length - 1)]
    formatted_column_list.insert(0, df_consensus.columns[0][0])

    # Code to extract and format statement date/period information from column
    statement_date_compile = re.compile(r'\d{4}\/\d{2}')    # Compile RegEx pattern to search for date items
    statement_date = [statement_date_compile.search(column) for column in formatted_column_list]    # List comprehension to collect the date items searched in column
    statement_date = [regex_group(item) for item in statement_date]    # List comprehension to "group" above list
    statement_date = [str_to_datetime(item) for item in statement_date]    # List comprehension to change string into datetime format

    # Code to extract and format accounting standard information from column
    acct_standard_compile = re.compile(r'\(\D{6}\)')    # Compile RegEx pattern to search for accounting standard items
    acct_standard = [acct_standard_compile.search(column) for column in formatted_column_list]    # List comprehension to collect the accounting standard items searched in column
    acct_standard = [regex_group(item) for item in acct_standard]    # List comprehension to "group" above list
    acct_standard = [regex_remove_brackets(item) for item in acct_standard]    # List comprehension to remove the brackets

    # Code to extract and format forecast indication information from column
    forecast_indication_compile = re.compile(r'\(E\)')    # Compile RegEx pattern to search for forecast indicators
    forecast_indication = [forecast_indication_compile.search(column) for column in formatted_column_list]    # List comprehension to collect the forecast indicator items searched in column
    forecast_indication = [regex_group(item) for item in forecast_indication]    # List comprehension to "group" above list
    forecast_indication = [regex_remove_brackets(item) for item in forecast_indication]    # List comprehension to remove the brackets
    forecast_indication = ['A' if item is None else item for item in forecast_indication]    # List comprehension to change None into 'A's

    # Use Zip function to create a list of tuples: (fin_item_date, acct_standard, forecast_indication)
    new_column = list(zip(statement_date, acct_standard, forecast_indication))

    # Change column to multi-level column based on the list of tuples "new_column"
    df_consensus.columns = pd.MultiIndex.from_tuples(new_column, names=('statement_period', 'accounting_standard', 'forecast_indication'))

    df_consensus.set_index(df_consensus.iloc[:, 0], inplace=True)    # Set first column as index
    df_consensus = df_consensus.loc[:, df_consensus.columns.labels[0] >= 0]    # Drop the first column (since it is already an index) and drop columns that doesn't have a column value
    df_consensus = df_consensus.unstack()    # Unstack the dataframe (results in a series)
    df_consensus = df_consensus.to_frame()    # Change the series into a dataframe
    df_consensus.reset_index(inplace=True)    # Reset the index; statement items are no longer the index
    df_consensus.columns = ['statement_period', 'accounting_standard', 'forecast_indication', 'financial_item', 'value']    # Re-name the column names with the list

    # Add a column of the financial items with designated codes
    financial_item_keys = {
        "매출액": 1100,
        "영업이익": 1300,
        "영업이익(발표기준)": 1301,
        "세전계속사업이익": 1500,
        "당기순이익": 1600,
        "당기순이익(지배)": 1601,
        "당기순이익(비지배)": 1602,
        "자산총계": 2100,
        "부채총계": 2200,
        "자본총계": 2300,
        "자본총계(지배)": 2301,
        "자본총계(비지배)": 2302,
        "자본금": 2303,
        "영업활동현금흐름": 3100,
        "투자활동현금흐름": 3200,
        "재무활동현금흐름": 3300,
        "CAPEX": 3201,
        "FCF": 3401,
        "이자발생부채": 2201,
        "영업이익률": 4130,
        "순이익률": 4160,
        "ROE(%)": 4163,
        "ROA(%)": 4164,
        "부채비율": 4220,
        "자본유보율": 4230,
        "EPS(원)": 4165,
        "PER(배)": 4501,
        "BPS(원)": 4301,
        "PBR(배)": 4502,
        "현금DPS(원)": 4331,
        "현금배당수익률": 4332,
        "현금배당성향(%)": 4333,
        "발행주식수(보통주)": 5000,
        "유보율": 4230,    # Term used in K-GAAP
        "현금배당성향": 4333    # Term used in K-GAAP
    }
    df_consensus["financial_item_code"] = df_consensus["financial_item"].map(financial_item_keys)
    new_column = df_consensus.columns.tolist()
    new_column = new_column[:2] + new_column[-1:] + new_column[2:-1]
    df_consensus = df_consensus[new_column]

    # Convert the accounting periods with designated codes
    accounting_standard_keys = {
        "IFRS연결": 1,
        "IFRS별도": 2,
        "GAAP연결": 3,
        "GAAP개별": 4
    }
    df_consensus["accounting_standard"].replace(accounting_standard_keys, inplace=True)

    # Insert ticker into the dataframe and rearrange the columns so that ticker comes first
    df_consensus['ticker'] = ticker
    new_column = df_consensus.columns.tolist()
    new_column = new_column[-1:] + new_column[:-1]
    df_consensus = df_consensus[new_column]

    update_date = sql_record_date    # SQL record date coming from user input in body of the program
    # update_date = date(2018, 5, 14)    # Date to manually input what is recorded as data download date on SQL
    df_consensus["update_date"] = update_date

    # Change financial item value units into ones
    df_consensus.loc[df_consensus.financial_item_code < 4000, 'value'] = df_consensus['value'].map(lambda x: x * (10**8))

    # Pull last updated financials from SQL into Pandas Dataframe
    sql = """
    SELECT
        *
    FROM naverfinance_consensus_financials
    WHERE ticker = "{a}";""".format(a=ticker)
    df_consensus_existing_latest = pd.read_sql(sql, con=sqlengine)

    df_consensus_existing_latest.sort_values(by=['update_date'], ascending=False, inplace=True)
    check_column = ["ticker", "statement_period", "financial_item_code", "accounting_standard", "forecast_indication"]
    df_consensus_existing_latest.drop_duplicates(subset=check_column, keep="first", inplace=True)

    # Combine the two Dataframes and delete items where everything but update_date is the same (if consensus changed, both latest existing and today's information kept)
    df_consensus_sql = df_consensus_existing_latest.append(df_consensus)
    df_consensus_sql['statement_period'] = pd.to_datetime(df_consensus_sql['statement_period'])
    check_column = ['ticker', 'statement_period', 'financial_item', 'financial_item_code', 'value', 'accounting_standard', 'forecast_indication']
    df_consensus_sql.drop_duplicates(subset=check_column, keep=False, inplace=True)

    # Drop all rows where value in  'update_date' column does not equal 'update_date' variable
    df_consensus_sql = df_consensus_sql[df_consensus_sql.update_date == update_date]

    # Return the created dataframe
    return(df_consensus_sql)


def regex_group(self):
    try:
        self = self.group()
    except AttributeError:
        pass
    return self


def regex_remove_brackets(self):
    left_bracket = re.compile(r"\(")
    right_bracket = re.compile(r"\)")
    try:
        self = re.sub(left_bracket, "", self)
        self = re.sub(right_bracket, "", self)
    except:
        self
    return self


def str_to_datetime(string):
    try:
        string = re.sub("\(E\)", "", string)    # Use RegEx to find "(E)" pattern and delete it
        string = datetime.strptime(string, '%Y/%m')    # Change the datetime from String datatype to Datetime datatype
        if string.month == 3 or string.month == 12:    # If statement to change day from first day of month to last day of month
            string = string.replace(day=31)
        elif string.month == 6 or string.month == 9:
            string = string.replace(day=30)
        string = string.date()    # "date" method from datetime used to show only year-month-date (not hours, minutes, and smaller units)
    except:
        pass    # Except used to accomodate for column names that are not meant to be dates
    return string


def execute_sql_file(filename):
    # Open and read the file as a single buffer
    fd = open(filename, 'r', encoding='UTF8')
    sql_file = fd.read()
    fd.close()

    # All SQL commands (split on ';')
    sql_commands = sql_file.split(';')

    # Execute every command from the input file
    for command in sql_commands[:-1]:    # sql_commands list has empty item in the end; [:-1] is used to avoid Operational Error
        # This will skip and report errors
        # For example, if the tables do not yet exist, this will skip over the DROP TABLE commands
        try:
            cursor.execute(command)
        except MySQLdb.OperationalError:
            print("Operational Error. Execution passed.")
            pass
        except MySQLdb.IntegrityError:
            print("Integrity Error. Execution passed.")
            pass


# SET UP REQUIRED SQL DATABASES
# Connecting to MySQL Schema 'marketdata' and open a cursor
sql_host = 'localhost'
sql_user = 'root'
sql_passwd = 'HanJae6387!'
sql_schema = 'marketdata'
conn = MySQLdb.connect(host=sql_host, user=sql_user, passwd=sql_passwd, db=sql_schema, charset='utf8')
cursor = conn.cursor()

# SQL query to create tables and pass the values for financial_item_keys, accounting_standard_keys, naverfinance_consensus_financials (using sql file)
execute_sql_file(r"C:\Users\HanJae\Desktop\accounting_standard_keys.sql")
execute_sql_file(r"C:\Users\HanJae\Desktop\financial_item_keys.sql")
execute_sql_file(r"C:\Users\HanJae\Desktop\naverfinance_consensus_financials.sql")
print("")  # To leave an empty line after messages
conn.commit()
cursor.close()

############### MAIN LOOP TO EXECUTE SCRAPING ###############
# Date Inputs and Defining Date Information
today = datetime.utcnow().replace(tzinfo=timezone.utc).astimezone(tz=None).date()
yesterday = today - timedelta(1)
latest_trading_day = today
while len(krx_marketdata_download(latest_trading_day.strftime('%Y%m%d'))) == 0:
    latest_trading_day = latest_trading_day - timedelta(1)

print("Today is " + calendar.day_name[today.weekday()] + " " + str(today))
print("Yesterday was " + calendar.day_name[yesterday.weekday()] + " " + str(yesterday))
print("Latest trading day was " + calendar.day_name[latest_trading_day.weekday()] + " " + str(latest_trading_day))

date_input = input("Today or Yesterday?    ").lower()
sql_record_date = 0
while (sql_record_date != today) or (sql_record_date != yesterday):
    if date_input == "today":
        sql_record_date = today
        break
    elif date_input == "yesterday":
        sql_record_date = today - timedelta(1)
        break
    else:
        date_input = input("Input again. Today or Yesterday")
print("'update_date' on SQL will be recorded as " + str(sql_record_date))

# Main non-date inputs for the scraping exercise
consensus_period = "Y"    # "Y" for Yearly "Q" for Quarterly
statement_type = 0    # 0 for Main Statement, 1 for K-GAAP Standalone, 2 for K-GAAP Consolidated, 3 for K-IFRS Standalone, 4 for K-IFRS Consolidated

# Deriving the list of tickers for scraping exercise
df_krx_excel = krx_marketdata_download(latest_trading_day.strftime('%Y%m%d'))    # DataFrame of KRX market data in Excel
ticker_list = df_krx_excel.iloc[:, 0]    # List of tickers
download_list = ticker_list  # Used to download partially

# For loop and scraping exercise result printing
sqlengine = create_engine('mysql+mysqldb://root:HanJae6387!@localhost/marketdata?charset=utf8')
consensus_na = []
download_count = 0
for ticker in download_list:
    df = naverfinance_financials_consensus(ticker, consensus_period, statement_type)    # DataFrame of Company's Financial Forecast Consensus from Naver Finance
    df.to_sql(name='naverfinance_consensus_financials', con=sqlengine, if_exists='append', index=False)
    download_count += 1
    print(str(ticker) + ": " + str(download_count) + " of " + str(len(ticker_list)) + " downloaded")

if len(consensus_na) == 0:
    print("All Tickers Scraped")
else:
    print(len(consensus_na))
    print(consensus_na)
