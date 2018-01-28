import pandas as pd
import numpy as np
import pandas_datareader.data as web
import datetime

print("Collecting Stock Data")

def daily_yield(company, start, end):

    stockdata = web.DataReader("{}".format(company), 'yahoo', start, end)['Adj Close']
    # calculating yields
    yields = stockdata/stockdata.shift(1)-1
    df_yields = yields.drop(yields.index[0])

    return(df_yields)

#print(daily_yield(MSFT, start, end))

print("Open Tweets")

def close2close_sentiments(df_sent, df_stock):

    sentiments = []
    dates = df_stock.index

    for date in dates:

        sent_c2c = {}

        sent_c2c['date'] = date
        one_day = datetime.timedelta(days=1)

        rows = df_sent.loc[df_sent['date'] == date and df_sent['time_slot'] == ['DURING' or 'BEFORE'] or
                           df_sent['date'] == date - one_day and df_sent['time_slot'] == 'AFTER']

        sent_c2c['sent_mean'] = np.mean(rows['SentimentLM'])

        sentiments.append(sent_c2c)

    df_c2c = pd.DataFrame(sentiments, index='date')

    return(df_c2c)

if __name__ == "__main__":
    company = "MSFT"
    start = datetime.datetime(2018, 1, 8)
    end = datetime.datetime(2018, 1, 9)
    df_stock = daily_yield(company, start, end)

    file_path = 'C:\\Users\\Open Account\\Documents\\BA_JonasIls\\Twitter_Streaming\\Test_SentimentsLM_MSFT.csv'
    tweets_file = pd.read_csv(file_path, encoding="utf-8")


    close2close_sentiments(tweets_file, df_stock)