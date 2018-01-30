import pandas as pd
import numpy as np
import pandas_datareader.data as web
from datetime import datetime, timedelta


def daily_yield(company, start, end):

    # parsing stockdata
    #stockdata = web.DataReader("{}".format(company), 'yahoo', start, end)['Adj Close']
    stockdata = pd.read_csv('Test_stockdata_MSFT.csv', index_col='Date')['Adj Close']

    # calculating yields
    yields = stockdata/stockdata.shift(1)-1
    df_yields = yields.drop(yields.index[0])

    return(df_yields)


# function to convert dates of typ sting to datetime format
def to_datetime(x):
    fmt = '%Y-%m-%d'
    x = datetime.strptime(x, fmt)
    return(x)

# main function to create dataframe containing daily yields and the daily sentiment
def close2close_sentiments(df_sent, df_stock):

    daily_sentiments = []
    dates = df_stock.index  # dates to search for in tweets dataframe

    df_sent['date'].apply(to_datetime)  # converting dates in tweets dataframe to datetime format

    for date in dates:

        sent_c2c = {}

        sent_c2c['date'] = date
        one_day = timedelta(days=1)
        date = to_datetime(date)

        # collecting all relevant tweets to create the c-2-c-sentiment and merging them to one dataframe
        rows_today = df_sent[(df_sent.date == date) & (df_sent.timeslot.isin(['during', 'before']))]
        rows_yesterday = df_sent[(df_sent.date == date - one_day) & (df_sent.timeslot == 'after')]
        rows = pd.concat([rows_today, rows_yesterday])

        # calculating the c-2-c-sentiment
        sent_c2c['sent_mean'] = np.mean(rows['SentimentLM'])
        sent_c2c['sent_std'] = np.std(rows['SentimentLM'])

        daily_sentiments.append(sent_c2c)

    df_c2c = pd.DataFrame(daily_sentiments)
    df_c2c = df_c2c.set_index('date')

    return(df_c2c)

def sent_stock_corr(df_c2cSent, df_c2cStock):

    df_results = pd.concat([df_c2cSent, df_c2cStock], axis=1, ignore_index=True)
    df_results = df_results.rename(columns={0:'Mean Sentiment', 1:'Std Sentiment', 2:'Daily Yields'}, inplace=True)
    corr_SentYields = df_results['Daily Yields'].corr(df_results['Mean Sentiment'])
    return (corr_SentYields)

if __name__ == "__main__":
    company = "MSFT"
    start = datetime(2018, 1, 8)
    end = datetime(2018, 1, 9)
    df_c2cStock = daily_yield(company, start, end)

    file_path = 'C:\\Users\\Open Account\\Documents\\BA_JonasIls\\Twitter_Streaming\\Test_SentimentsLM_MSFT.csv'
    df_tweets = pd.read_csv(file_path, encoding="utf-8")

    df_c2cSent = close2close_sentiments(df_tweets, df_c2cStock)
    corr_SentYields = sent_stock_corr(df_c2cSent, df_c2cStock)

    print(df_c2cSent, corr_SentYields)