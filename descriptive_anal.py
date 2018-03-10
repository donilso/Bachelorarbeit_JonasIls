import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.mlab as mlab
import calendar
from datetime import datetime, timedelta
import sent_stock_corr as ssc



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

    df_sentstock = ssc.close2close_sentiments(df_sent, sent_dict, df_stock, 0, 0, volume_filter=True, sentiment_filter=True)
    df_sentstock = pd.concat([df_sentstock, df_stock], axis=1)
    df_sentstock.to_csv('C:\\Users\\Open Account\\Documents\\BA_JonasIls\\Twitter_Streaming\\Sentiment_Dataframes\\20180101_20180217\\C2C_Dataframes\\20180101_20180217_C2C_{}'.format(company), index_label='date')
    return(df_sentstock)


def time_to_int(ts):
    return 10000 * ts.hour + 100 * ts.minute + ts.second


def get_TBSentiment(text):
    '''Function to calculate a sentiment score based on the textblob library'''
    analysis = TextBlob(text)
    SentimentTB = analysis.sentiment.polarity
    return(SentimentTB)


def open_df_c2c(company, sentiment_dict, vol_min, sent_min):
    file_path = 'C:\\Users\\Open Account\\Documents\\BA_JonasIls\\Twitter_Streaming\\Sentiment_Dataframes\\20180101_20180217\\C2C_Dataframes\\20180101_20180217_C2C{}_{}_{}vol_{}sen.csv'.format(company, sentiment_dict, vol_min, sent_min)
    return pd.read_csv(file_path, encoding="utf-8", parse_dates=True)


def open_df_sent(company):
    file_path = 'C:\\Users\\Open Account\\Documents\\BA_JonasIls\\Twitter_Streaming\\Sentiment_Dataframes\\20180101_20180217\\20180101_20180217_SentimentDataframes_{}'.format(company)
    df_tweets = pd.read_csv(file_path, encoding="utf-8")
    df_tweets['date'] = pd.to_datetime(df_tweets['date'], errors='coerce')
    return df_tweets


def plot_sentiment_dist(company, sentiment_dict):

    print('Open DF_SENT')
    df_sent = open_df_sent(company)
    df_sent = df_sent.dropna()
    df_sent=df_sent['{}'.format(sentiment_dict)]

    print('set parameters')
    mu = df_sent.mean()  # mean of distribution
    sigma = df_sent.std()  # standard deviation of distribution
    x = df_sent
    num_bins = 20

    print('create plot')
    fig, ax = plt.subplots()

    # the histogram of the data
    n, bins, patches = ax.hist(x, num_bins, normed=1, rwidth=0.8)

    # add a 'best fit' line
    y = mlab.normpdf(bins, mu, sigma)
    ax.plot(bins, y, '--')
    ax.set_xlabel('Sentiment Values')
    ax.set_ylabel('Probability density')
    ax.set_title('Histogram of {}'.format(sentiment_dict))

    # Tweak spacing to prevent clipping of ylabel
    fig.tight_layout()
    plt.savefig('C:\\Users\\Open Account\\Documents\\BA_JonasIls\\Literatur & Analysen\\Plots\\Sent_Distribution\\Twitter_PlotSentDist_{}'.format(sentiment_dict), bbox_inches='tight')
    plt.show()


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


def plot_tweetcount_byweekday(company):
    df_tweets = open_df_sent(company)
    df_tweets = df_tweets[['date', 'time_adj']]
    print('opened')
    #df_tweets['date'] = pd.to_datetime(df_tweets['date'], errors='coerce')
    df_tweets['time_adj'] = pd.to_datetime(df_tweets['time_adj'], errors='coerce')
    df_tweets = df_tweets.loc[df_tweets.time_adj.notnull()]
    df_tweets['hour'] = df_tweets['time_adj'].dt.hour
    #df_tweets['time_adj'] = [timestamp.time() for timestamp in df_tweets['time_adj']]
    df_tweets['dow'] = df_tweets['date'].dt.dayofweek
    df_tweets = df_tweets.loc[df_tweets['dow'].isin(list(range(5, 7)))]
    print('time formated')

    # sum of counts for all combinations
    df_tweets = df_tweets.reset_index()
    df_tweets = df_tweets.groupby(['date', 'hour']).count()
    df_tweets = df_tweets.reset_index().groupby(['hour']).mean()
    #df_tweets = df_tweets.groupby(['date', 'hour']).count()
    print('created means')

    #bins = df_tweets['hour'].unique().sort()
    df_tweets.plot(kind='bar', legend=False)
    plt.xlabel('Hour')
    plt.ylabel('Number of Tweets')
    plt.title('Tweets per Hour on Weekends')
    plt.xticks(list(range(0, 24)), ['00:00 - 00:59', '01:00 - 01:59', '02:00 - 02:59', '03:00 - 03:59', '04:00 - 04:59', '05:00 - 05:59', '06:00 - 06:59', '07:00 - 07:59', '08:00 - 08:59', '09:00 - 09:59', '10:00 - 10:59', '11:00 - 11:59', '12:00 - 12:59', '13:00 - 13:59', '14:00 - 14:59', '15:00 - 15:59', '16:00 - 16:59', '17:00 - 17:59', '18:00 - 18:59', '19:00 - 19:59', '20:00 - 20:59', '21:00 - 21:59', '22:00 - 22:59', '23:00 - 23:59'])
    #plt.xticks([0, 1, 2, 3, 4, 5, 6], ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun'])

    plt.savefig('C:\\Users\\Open Account\\Documents\\BA_JonasIls\\Literatur & Analysen\\Plots\\PlotTweetCount_Hour_Weekend', bbox_inches='tight')
    plt.show()


def plot_twittervsstock(company, df_sentstock, stock_var, twi_var):
    #df_sentstock=df_sentstock.reset_index()
    #df_sentstock=df_sentstock.set_index('date')
    df_sentstock.index = df_sentstock.index.astype(str)
    fig, ax1 = plt.subplots()
    ax2 = ax1.twinx()
    df_sentstock['{}'.format(twi_var)].plot(kind='bar', color='y', ax=ax1)
    df_sentstock['{}'.format(stock_var)].plot(kind='line', marker='d', ax=ax2)
    ax1.yaxis.tick_right()
    ax2.yaxis.tick_left()
    plt.xlabel('Trading Day')
    plt.title('{} vs {}'.format(twi_var, stock_var))
    plt.legend(loc=0)

    plt.savefig('C:\\Users\\Open Account\\Documents\\BA_JonasIls\\Literatur & Analysen\\Plots\\Twitter_vs_Stock\\PlotTwiStock{}_{}_{}'.format(company, stock_var, twi_var), bbox_inches='tight')


def scatterplot(company, stock_var, twi_var, sentiment_dict, vol_min, sent_min):
    df = open_df_c2c(company, sentiment_dict, vol_min, sent_min)
    df.index = df.index.astype(str)

    var_std = ['tweet_count', 'count_pos', 'count_neg']
    for x in var_std:
        mean = df['{}'.format(x)].mean()
        std = df['{}'.format(x)].std()
        df['{}_std'.format(x)] = ((df['{}'.format(x)] - mean) / std)

    df = df[['{}'.format(stock_var), '{}'.format(twi_var)]]

    df.plot(x=stock_var, y=twi_var, style='o')
    plt.xlabel('{}'.format(stock_var))
    plt.ylabel('{}'.format(twi_var))
    plt.title('{} vs. {}'.format(stock_var, twi_var))
    plt.savefig('C:\\Users\\Open Account\\Documents\\BA_JonasIls\\Literatur & Analysen\\Plots\\ScatterPlots\\PlotScatter{}_{}_{}'.format(company, stock_var, twi_var), bbox_inches='tight')
    plt.show()


LM = 'SentimentLM'
GI = 'SentimentGI'
HE = 'SentimentHE'
TB = 'SentimentTB'


# Plot Twitter vs Stock
stock_var = 'volume_std'
twi_var = ['tweet_count_std', 'count_neg_std', 'count_pos_std']

#plot_tweetcount_byweekday(companies)
#plot_twittervsstock_bydate(company='HD', sentiment_dict=GI, stock_var=stock_var, twi_var=twi_var)

companies = ['$MSFT', '$MMM', '$AXP', '$BA', '$CAT', '$CVX', '$CSCO', '$KO', '$DWDP', '$DIS', '$XOM', '$GE', '$GS', '$HD', '$IBM', '$INTC', '$JNJ', '$JPM', '$MCD', '$MRK', '$NKE', '$PFE', '$PG', '$TRV', '$UTX', '$UNH', '$VZ', '$V', '$WMT']
companies = [company.replace('$', '') for company in companies]

#plot_tweetcount_byweekday('AllStocks')

#plot_tweetcount_byweekday(companies)

#for company in companies:
#    print(company)
#    df_c2c = open_df_c2c(company)
#    scatterplot(company, df_c2c, stock_var, twi_var)
#    plot_twittervsstock(company, df_c2c, stock_var, twi_var)

for var in twi_var:
    scatterplot('AllStocks', sentiment_dict=GI, stock_var=stock_var, twi_var=var, vol_min=0, sent_min=0)

