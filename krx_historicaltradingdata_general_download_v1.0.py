import requests
import numpy as np
import pandas as pd
import MySQLdb
from sqlalchemy import create_engine
from io import BytesIO
import datetime
from datetime import datetime
from datetime import date


def krx_marketdata_download(data_date=None):  # Download market data for all KRX listed stocks
    # If parameter is left empty, assume today's date
    if data_date is None:
        date_str = datetime.today().strftime('%Y%m%d')
    else:
        date_str = data_date.strftime('%Y%m%d')

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

    # Change column names to English
    column_dict = {
        '종목코드': 'ticker',
        '종목명': 'company_name',
        '현재가': 'price_close',
        '대비': 'price_change',
        '등락률': 'price_change_pct',
        '거래량': 'volume',
        '거래대금': 'trading_value',
        '시가': 'price_open',
        '고가': 'price_high',
        '저가': 'price_low',
        '시가총액': 'marketcap',
        '시가총액비중(%)': 'market_weight_pct',
        '상장주식수(천주)': 'shares_issued',    # Actual data is not in thousands
        '외국인 보유주식수': 'foreign_shareholding',
        '외국인 지분율(%)': 'foreign_shareholding_pct'
    }
    df.rename(columns=column_dict, inplace=True)

    # Create new column with date of data and re-order columns
    df["data_date"] = data_date
    columns = df.columns.tolist()
    columns = columns[:2] + columns[-1:] + columns[2:len(columns) - 1]
    df = df[columns]

    # Change percentage data into decimal integer form
    df['price_change_pct'] = df.price_change_pct.apply(lambda x: float(x) / 100)
    df['market_weight_pct'] = df.market_weight_pct.apply(lambda x: float(x) / 100)
    df['foreign_shareholding_pct'] = df.foreign_shareholding_pct.apply(lambda x: float(x) / 100)

    return df


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


# Connecting to MySQL Schema 'marketdata' and open a cursor
sql_host = 'localhost'
sql_user = 'root'
sql_passwd = 'HanJae6387!'
sql_schema = 'marketdata'
conn = MySQLdb.connect(host=sql_host, user=sql_user, passwd=sql_passwd, db=sql_schema, charset='utf8')
cursor = conn.cursor()

execute_sql_file(r"C:\Users\HanJae\Desktop\KRX_HistoricalData_Download_Codes\krx_tradingdata_createtable.sql")

conn.commit()
cursor.close()

# Create sqlengine to pull and insert dataframe into SQL
sqlengine = create_engine('mysql+mysqldb://root:HanJae6387!@localhost/marketdata?charset=utf8')

# Pull number of tickers for each date (which is to check if data already exists in database or not)
sql = """
SELECT
    data_date, count(*) AS 'data_count'
FROM krxmarketdata
GROUP BY data_date
;"""
df_sqldatacount = pd.read_sql(sql, con=sqlengine)
df_sqldatacount.set_index('data_date', inplace=True)

# Date information
today = datetime.utcnow().replace(tzinfo=timezone.utc).astimezone(tz=None).date()
latest_sql_date = df_sqldatacount.index.max()
print("Today is " + calendar.day_name[today.weekday()] + " " + str(today))
print("Latest date in SQL database is " + calendar.day_name[latest_sql_date.weekday()] + " " + str(latest_sql_date))

# USER INPUT 1: Designate the dates to download market data
start_date = 0
start_date_input = input("Download KRX market data from a week before the latest date in SQL database?\nInput 'Yes' (or 'Y') or 'No' (or 'N'): ").lower()    # Used week prior in case any new information has been updated
while start_date == 0:
    if start_date_input == "yes" or start_date_input == 'y':
        start_date = latest_sql_date - timedelta(7)    # Timedelta used to download data beginning a week prior
    elif start_date_input == 'no' or start_date_input == 'n':
        start_date_year = input("Input YEAR for data download start date: ")
        start_date_month = input("Input MONTH for data download start date: ")
        start_date_day = input("Input DATE for data download start date: ")
        start_date = date(int(start_date_year), int(start_date_month), int(start_date_day))
    else:
        start_date_input = input("Please input again.\nDownload KRX market data from a week before the latest date in SQL database?\nInput 'Yes' (or 'Y') or 'No' (or 'N'): ")
print("Selected start date is "+calendar.day_name[start_date.weekday()]+" "+str(start_date)+"\n")

end_date = 0
end_date_input = input("Download KRX market data up to today?\nInput 'Yes' (or 'Y') or 'No' (or 'N'): ").lower()
while end_date == 0:
    if end_date_input == "yes" or end_date_input == 'y':
        end_date = today
    elif end_date_input == "no" or end_date_input == 'n':
        end_date_year = input("Input YEAR for data download end date: ")
        end_date_month = input("Input MONTH for data download end date: ")
        end_date_day = input("Input DATE for data download end date: ")
        end_date = date(int(end_date_year), int(end_date_month), int(end_date_day))
    else:
        end_date_input = input("Please input again.\nDownload KRX market data up to today?\nInput 'Yes' (or 'Y') or 'No' (or 'N'): ")
print("Selected end date is "+calendar.day_name[end_date.weekday()]+" "+str(end_date)+"\n")

# Statistics variables for download progress and sanity check
download_count = 0
download_total = len(dates)
sanity_check = []

# For Loop to go through the dates
for data_date in dates:
    download_count += 1
    download_percentage = (download_count / download_total) * 100
    df_data = krx_marketdata_download(data_date)
    if data_date.date() in df_sqldatacount.index:
        print("{a}:{b:6d}{c:10.3f}% Exists in Database".format(a=data_date.strftime('%Y%m%d'), b=len(df_data), c=download_percentage))
        if len(df_data) != df_sqldatacount.loc[data_date.date()][0]:
            sanity_check.append(data_date)
    elif len(df_data) == 0:
        print("{a}:{b:6d}{c:10.3f}% No Trading Day".format(a=data_date.strftime('%Y%m%d'), b=len(df_data), c=download_percentage))
    else:
        df_data.to_sql(name='krxmarketdata', con=sqlengine, if_exists='append', index=False)
        print("{a}:{b:6d}{c:10.3f}% Downloaded".format(a=data_date.strftime('%Y%m%d'), b=len(df_data), c=download_percentage))

print("Download Complete")
if len(sanity_check) == 0:
    print('Sanity Check Cleared')
else:
    print(sanity_check)
