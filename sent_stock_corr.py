import pandas as pd
import numpy as np
import pandas_datareader.data as web
from datetime import datetime, timedelta
from textblob import TextBlob

def get_TBSentiment(text):
    '''Function to calculate a sentiment score based on the textblob library'''
    analysis = TextBlob(text)
    SentimentTB = analysis.sentiment.polarity
    return(SentimentTB)


def open_df_sent(company):
    file_path = 'C:\\Users\\Open Account\\Documents\\BA_JonasIls\\Twitter_Streaming\\20180101Test_SentimentsLM_{}.csv'.format(company)
    df_tweets = pd.read_csv(file_path, encoding="utf-8")
    df_tweets['SentimentTB'] = get_TBSentiment(str(df_tweets['text_clean']))

    return(df_tweets)


def date_start(df_sent):
    return(df_sent.date.values[0])


def date_end(df_sent):
    return(df_sent.date.values[len(df_sent)-1])


def daily_yield(company, start, end):
    '''Function to parse daily stock quotes and calculate daily yields'''
    # parsing stockdata
    #stockdata = web.DataReader("{}".format(company), 'yahoo', start, end)['Adj Close']
    stockdata = pd.read_csv('Test_stockdata_MSFT.csv', index_col='Date')['Adj Close']

    # calculating yields
    yields = stockdata/stockdata.shift(1)-1
    df_yields = yields.drop(yields.index[0])
    return(df_yields)


# function to convert dates of typ sting to datetime format
def to_datetime(date_str):
    '''Utiliy function to convert strings to datetime format'''
    try:
        fmt = '%Y-%m-%d'
        date_datetime = datetime.strptime(date_str, fmt)
        return(date_datetime)

    except:
        return(date_str)


# main function to create dataframe containing daily yields and the daily sentiment
def polarity(dataframe, sent_dict, pol):
    '''Function to calculate the weighted average of positive / negative polarity depending on a user's followers count.
    !!! DEFAULT IS POSITIVE !!! For negative Polarity pol == False'''

    if pol == True:
        rows = dataframe.loc[dataframe[sent_dict] > 0]
    else:
        rows = dataframe.loc[dataframe[sent_dict] < 0]

    rows['pol_w'] = rows[sent_dict] * rows['user_followers']
    followers_count = rows['user_followers'].sum()
    polarity_mean_w = rows['sent_w'].sum() / followers_count

    return(polarity_mean_w)


def threshhold (df_sent, sent_dict, sent_min):
    sent_dict = '{}'.format(sent_dict)
    return df_sent.loc[(df_sent[sent_dict] >= sent_min) | (df_sent[sent_dict] <= (sent_min*(-1)))]


def close2close_sentiments(df_sent, sent_dict, df_stock):

    sent_dict = '{}'.format(sent_dict)

    daily_sentiments = []
    dates = df_stock.index  # dates to search for in tweets dataframe

    for date in dates:

        sent_c2c = {}

        # adding column of weighted sentiment depending on the followers count
        df_sent['sent_w'] = df_sent[sent_dict] * df_sent['user_followers']

        #today = date
        today = to_datetime(date)  # has to be used if you read stocks from csv and not directly from pd.DataReader
        sent_c2c['date'] = today

        # defining timedeltas select c2c-rows
        one_day = timedelta(days=1)
        two_days = timedelta(days=2)
        three_days = timedelta(days=3)

        yesterday = today - one_day
        df_sent.date = pd.to_datetime(df_sent.date)

        if today.weekday() != 0:

            yesterday = today-one_day
            rows_today = df_sent.loc[(df_sent.date == today) & (df_sent.timeslot.isin(['during', 'before']))]
            rows_yesterday = df_sent.loc[(df_sent.date == yesterday) & (df_sent.timeslot == 'after')]
            rows = pd.concat([rows_today, rows_yesterday])

        else:
            rows = df_sent.loc[(df_sent.date == today) & (df_sent.timeslot.isin(['during', 'before']))]

### Whole Weekend to calculate Monday sentiment
        #if today.weekday()!= 0:

#            yesterday = today - one_day
#            # filter rows for c2c sentiment
#            rows_today = df_sent.loc[(df_sent.date == today) & (df_sent.timeslot.isin(['during', 'before']))]
#            rows_yesterday = df_sent.loc[(df_sent.date == yesterday) & (df_sent.timeslot == 'after')]
#            rows = pd.concat([rows_today, rows_yesterday])

#        else:
#            yesterday = [today - one_day, today - two_days]
#            rows_today = df_sent.loc[(df_sent.date == today) & (df_sent.timeslot.isin(['during', 'before']))]
#            rows_weekend = df_sent.loc[(df_sent.date.isin(yesterday)) or ((df_sent.date == today - three_days) & (df_sent.timeslot == 'after'))]
#            rows = pd.concat([rows_today, rows_weekend])

        # calculating positive and negative polarity (weighted average)
        sent_c2c['pol_pos'] = polarity(rows, sent_dict, True)
        sent_c2c['pol_neg'] = polarity(rows, sent_dict, False)

        # calculating the c-2-c-sentiment
        sent_c2c['sent_mean'] = np.mean(rows[sent_dict])
        sent_c2c['sent_std'] = np.std(rows[sent_dict])

        # getting the number of tweets, positive tweets as well as negative tweets
        sent_c2c['tweet_count'] = len(rows)
        sent_c2c['count_pos'] = len(rows.loc[rows[sent_dict] <= 0])
        sent_c2c['count_neg'] = len(rows.loc[rows[sent_dict] > 0])

        # calculating the c-2-c-sentiment depending on the followers
        followers_count = rows['user_followers'].sum()
        sent_c2c['sent_mean_w'] = rows['sent_w'].sum() / followers_count

        daily_sentiments.append(sent_c2c)

    df_c2c = pd.DataFrame(daily_sentiments)
    df_c2c = df_c2c.set_index('date')

    return(df_c2c)


def sent_stock_corr(df_c2cSent, df_c2cStock):

    df_results = pd.concat([df_c2cSent, df_c2cStock], axis=1)
    corr_SentYields = df_results['Adj Close'].corr(df_results['sent_mean_w'])

    #df_results = df_results.rename(columns={0: 'Mean Sentiment', 1: 'Std Sentiment', 2: 'Daily Yields'}, inplace=True)
    #corr_SentYields = df_results['Daily Yields'].corr(df_results['Mean Sentiment'])
    return (corr_SentYields)


def main_correlation(companies, sentiment_dictionaries):
    correlations = []
    companies_list = [company.replace('$', '') for company in companies]

    for company in companies_list:

        # open tweets
        df_tweets = open_df_sent(company)

        # calculate timeperiod to parse stock quotes
        start = date_start(df_tweets)
        end = date_end(df_tweets)

        # parse stock quotes
        df_c2cStock = daily_yield(company, start, end)

        for sentiment_dict in sentiment_dictionaries:
            corr_SentYields = {}
            df_tweets = threshhold(df_tweets, sentiment_dict, 0.2)
            df_c2cSent = close2close_sentiments(df_tweets, sentiment_dict, df_c2cStock)
            corr_SentYields['company'] = company
            corr_SentYields['sentiment_dict'] = sentiment_dict
            corr_SentYields['correlation'] = sent_stock_corr(df_c2cSent, df_c2cStock)

            correlations.append(corr_SentYields)

    df_corr = pd.DataFrame(correlations).set_index('company')

    return(df_corr)


def main_sentiments(companies, sentiment_dictionaries, sent_min):

    companies_list = [company.replace('$', '') for company in companies]

    for company in companies_list:
        df_tweets = open_df_sent(company)

        start = date_start(df_tweets)
        end = date_end(df_tweets)
        df_stock = daily_yield(company, start, end)

        for sent_dict in sentiment_dictionaries:
            df_tweets = threshhold(df_tweets, sent_dict, sent_min)
            df_c2c = close2close_sentiments(df_tweets, sent_dict, df_stock)

            return(df_c2c)



def analyze_corr(df_corr):
    corr_mean = df_corr.groupby('sentiment_dict')['correlation'].mean()
    corr_std = df_corr.groupby('sentiment_dict')['correlation'].std()

    df_corr_analysis = pd.concat([corr_mean, corr_std], axis=1)
    df_corr_analysis.rename(columns={0: 'corr_mean', 1: 'corr_std'}, inplace=True)

    return(df_corr_analysis)

if __name__ == "__main__":

    companies = ['$MSFT', '$MMM', '$AXP', '$AAPL', '$BA', '$CAT', '$CVX', '$CSCO', '$KO', '$DWDP', '$DIS', '$XOM', '$GE', '$GS', '$HD', '$IBM', '$INTC', '$JNJ', '$JPM', '$MCD', '$MRK', '$NKE', '$PFE', '$PG', '$TRV', '$UTX', '$UNH', '$VZ', '$V', '$WMT']

    LM = 'SentimentLM'
    GI = 'SentimentGI'
    HE = 'SentimentHE'
    QDAP = 'SentimentQDAP'
    TB = 'SentimentTB'
    sentiment_dicts = [LM, GI, HE, QDAP, TB]

    df_corr = main_correlation(companies, sentiment_dicts)
    print(analyze_corr(df_corr))
    print(main_sentiments(companies, sentiment_dicts, 0.2))
