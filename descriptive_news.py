import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.mlab as mlab
from textblob import TextBlob
import calendar
from datetime import datetime, timedelta
import analysis_news as an
import math



def create_c2c(company, sent_dict):
    print('Open {}'.format(company))
    df_tweets = open_df_sent(company)
    # calculate timeperiod to parse stock quotes
    start = ssc.date_start(df_tweets)
    end = ssc.date_end(df_tweets)
    # parse stock quotes
    df_stock = ssc.daily_yield(company, start, end)

    tuple = (df_tweets, df_stock)

    df_sent = tuple[0]
    df_stock = tuple[1]

    df_sentstock = an.close2close_sentiments(df_sent, sent_dict, df_stock, 0)
    df_sentstock = pd.concat([df_sentstock, df_stock], axis=1)
    df_sentstock.to_csv('C:\\Users\\Open Account\\Documents\\BA_JonasIls\\Newsfeeds\\Sentiment_Dataframe\\C2C_Dataframes\\NewsC2C_{}'.format(company), index_label='date')
    return(df_sentstock)


def time_to_int(ts):
    return 10000 * ts.hour + 100 * ts.minute + ts.second


def get_TBSentiment(text):
    '''Function to calculate a sentiment score based on the textblob library'''
    analysis = TextBlob(text)
    SentimentTB = analysis.sentiment.polarity
    return(SentimentTB)


def open_df_c2c(company, sentiment_dict, vol_min):
    file_path = 'C:\\Users\\Open Account\\Documents\\BA_JonasIls\\Newsfeeds\\Sentiment_Dataframes\\C2C_Dataframes\\NewsC2C_{}_{}_{}vol'.format(company, sentiment_dict, vol_min)
    return pd.read_csv(file_path, encoding="utf-8", parse_dates=True)


def open_df_sent(company):
    file_path = 'C:\\Users\\Open Account\\Documents\\BA_JonasIls\\Newsfeeds\\Sentiment_Dataframes\\NewsSentimentDataframes_{}.csv'.format(company)
    df_tweets = pd.read_csv(file_path, encoding="utf-8")
    df_tweets['date'] = pd.to_datetime(df_tweets['date'])
    #df_tweets['SentimentTB'] = df_tweets['article_clean'].apply(get_TBSentiment)
    #df_tweets.to_csv(file_path, encoding='utf-8')

    return df_tweets


def plot_sentiment_dist(list_of_companies, sentiment_dict):
    raw_data = []
    for company in list_of_companies:
        print('Open {}'.format(company))
        df_tweets = open_df_sent(company)
        raw_data.append(df_tweets)

    df_sent = pd.concat(raw_data)
    df_sent = df_sent['{}'.format(sentiment_dict)]
    #df_sent['rounded'] = df_sent['{}'.format(sentiment_dict)].apply()

    # example data
    mu = df_sent.mean()  # mean of distribution
    sigma = df_sent.std()  # standard deviation of distribution
    x = df_sent
    num_bins = 20

    fig, ax = plt.subplots()

    # the histogram of the data
    n, bins, patches = ax.hist(x, num_bins, normed=1, rwidth=0.6)

    # add a 'best fit' line
    y = mlab.normpdf(bins, mu, sigma)
    ax.plot(bins, y, '--')
    ax.set_xlabel('Sentiment Values')
    ax.set_ylabel('Probability density')
    ax.set_title('Histogram of {}'.format(sentiment_dict))

    # Tweak spacing to prevent clipping of ylabel
    fig.tight_layout()

    plt.savefig('C:\\Users\\Open Account\\Documents\\BA_JonasIls\\Newsfeeds\\Plots\\Sent_Distribution\\PlotSentDist_{}'.format(sentiment_dict), bbox_inches='tight')


def plot_tweetcount_bytradingday(list_of_companies):
    raw_data = []
    for company in list_of_companies:
        df_tweets = open_df_sent(company, ['time_adj', 'date'])
        raw_data.append(df_tweets)

    df_tweets = pd.concat(raw_data)

    df_tweets['date'] = pd.to_datetime(df_tweets['date'])
    df_tweets['time_adj'] = pd.to_datetime(df_tweets['time_adj'])
    df_tweets['time_adj'] = df_tweets['time_adj']
    df_tweets['hour'] = df_tweets['time_adj'].dt.hour
    df_tweets['time_adj'] = [timestamp.time() for timestamp in df_tweets['time_adj']]
    df_tweets['weekday_number'] = df_tweets['date'].dt.dayofweek
    df_tweets['weekday_name'] = [calendar.day_name[day] for day in df_tweets['weekday_number']]
    df_tweets = df_tweets.reset_index()
    df_tweets = df_tweets.groupby(['weekday_number', 'hour']).count()
    # df_tweets = df_tweets.set_index('time_adj')
    # df_tweets = df_tweets.groupby(pd.Grouper(level='time_adj', freq='3h')).count()
    # df_tweets.groupby([pd.Grouper(freq='H'), 'weekday_name']).count()

    # bins = df_tweets['hour'].unique().sort()
    df_tweets.unstack().plot(kind='bar', legend=False)
    plt.xlabel('Weekday')
    plt.ylabel('Number of Tweets')
    plt.title('Tweets per Weekday')
    plt.xticks([0, 1, 2, 3, 4, 5, 6], ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun'])

    plt.show()


def plot_tweetcount_byweekday(list_of_companies):
    raw_data = []
    for company in list_of_companies:
        print(company)
        df_tweets = open_df_sent(company)
        df_tweets = df_tweets[['date', 'time_adj']]
        raw_data.append(df_tweets)

    df_tweets = pd.concat(raw_data)
    #df_tweets.to_csv('C:\\Users\\Open Account\\Documents\\BA_JonasIls\\Twitter_Streaming\\Sentiment_Dataframes\\20180101_20180217\\20180101_20180217_DfSent_AllStocks')
    df_tweets['date'] = pd.to_datetime(df_tweets['date'])
    df_tweets['time_adj'] = pd.to_datetime(df_tweets['time_adj'])
    df_tweets['time_adj'] =df_tweets['time_adj']
    df_tweets['hour'] = df_tweets['time_adj'].dt.hour
    df_tweets['time_adj'] = [timestamp.time() for timestamp in df_tweets['time_adj']]
    df_tweets['weekday_number'] = df_tweets['date'].dt.dayofweek
    df_tweets['weekday_name'] = [calendar.day_name[day] for day in df_tweets['weekday_number']]
    df_tweets = df_tweets.reset_index()
    print(df_tweets['date'].unique())
    df_tweets = df_tweets.groupby(['date', 'hour']).count()
    df_tweets.to_excel('C:\\Users\\Open Account\\Documents\\BA_JonasIls\\Literatur & Analysen\\Plots\\Postings_Stunde.xls')
    #df_tweets = df_tweets.set_index('time_adj')
    #df_tweets = df_tweets.groupby(pd.Grouper(level='time_adj', freq='3h')).count()
    #df_tweets.groupby([pd.Grouper(freq='H'), 'weekday_name']).count()

    #bins = df_tweets['hour'].unique().sort()
    df_tweets.unstack().plot(kind='bar', legend=False)
    plt.xlabel('Weekday')
    plt.ylabel('Number of Tweets')
    plt.title('Tweets per Weekday')
    plt.xticks([0, 1, 2, 3, 4, 5, 6], ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun'])

    plt.savefig('C:\\Users\\Open Account\\Documents\\BA_JonasIls\\Literatur & Analysen\\Plots\\PlotTweetCount_Weekday', bbox_inches='tight')


def plot_newsvsstock(company, sentiment_dict, vol_min, stock_var, twi_var):
    df = open_df_c2c(company, sentiment_dict, vol_min)
    df.index = df.index.astype(str)
    fig, ax1 = plt.subplots()
    ax2 = ax1.twinx()
    df['{}'.format(twi_var)].plot(kind='bar', color='y', ax=ax1)
    df['{}'.format(stock_var)].plot(kind='line', marker='d', ax=ax2)
    ax1.yaxis.tick_right()
    ax2.yaxis.tick_left()
    ax1.set_ylabel('{}'.format(twi_var))
    ax2.set_ylabel('{}'.format(stock_var))
    plt.xlabel('Trading Day')
    plt.title('{} vs {}'.format(twi_var, stock_var))
    plt.legend(loc=0)

    plt.savefig('C:\\Users\\Open Account\\Documents\\BA_JonasIls\\Newsfeeds\\Plots\\\\PlotNewsStock{}_{}_{}'.format(company, stock_var, twi_var), bbox_inches='tight')
    plt.show()


def scatterplot(company, sentiment_dict, stock_var, twi_var):
    df = open_df_c2c(company, sentiment_dict, 0)
    print(len(df))
    df.index = df.index.astype(str)
    #plt.scatter(df_sentstock['{}'.format(stock_var)], df_sentstock['{}'.format(twi_var)])

    df.plot(x=stock_var, y=twi_var, style='o')
    plt.xlabel('{}'.format(stock_var))
    plt.ylabel('{}'.format(twi_var))
    plt.title('{} vs. {}'.format(stock_var, twi_var))
    plt.savefig('C:\\Users\\Open Account\\Documents\\BA_JonasIls\\Newsfeeds\\Plots\\PlotScatter{}_{}_{}'.format(company, stock_var, twi_var), bbox_inches='tight')
    plt.show()

LM = 'SentimentLM'
GI = 'SentimentGI'
HE = 'SentimentHE'
TB = 'SentimentTB'

# Plot Twitter vs Stock
stock_var = 'volume_std'
twi_var = 'news_count'

sent_dicts = [GI, LM, TB]

companies = ['$MSFT', '$MMM', '$AAPL',  '$AXP', '$BA', '$CAT', '$CVX', '$CSCO', '$KO', '$DWDP', '$DIS', '$XOM', '$GE', '$GS', '$HD', '$IBM', '$INTC', '$JNJ', '$JPM', '$MCD', '$MRK', '$NKE', '$PFE', '$PG', '$TRV', '$UTX', '$UNH', '$VZ', '$V', '$WMT']
companies = [company.replace('$', '') for company in companies]

#for sent_dict in sent_dicts:
#    plot_sentiment_dist(companies, sent_dict)

#for var in twi_var:
#    scatterplot('AllStocks', GI, stock_var, var)
for company in companies:
    plot_newsvsstock(company, GI, 0, stock_var, twi_var)