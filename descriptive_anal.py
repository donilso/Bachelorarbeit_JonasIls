import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.mlab as mlab
import calendar
from datetime import datetime, timedelta
import seaborn as sns


def get_trading_hour(time_int):

    start = 100000
    hours = range(1, 7)
    trading_hours = list()

    for i in hours:
        x = start + 10000 * (i - 1)
        y = x + 10000 - 40
        hour = range(x, y)
        trading_hours.append(hour)

    for hour in trading_hours:
        if time_int in hour:
            return trading_hours.index(hour) + 1
        else:
            continue


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
    bad_days = ['2018-01-04', '2018-01-03', '2018-01-02', '2018-01-01', '2018-01-25', '2018-01-26', '2018-01-27',
                '2018-02-02', '2018-02-03', '2018-02-06', '2018-02-10']
    file_path = 'C:\\Users\\Open Account\\Documents\\BA_JonasIls\\Twitter_Streaming\\Sentiment_Dataframes\\spam_cleaned\\C2C_Dataframes\\c2c_spamcleaned{}_{}_Vol{}_Sent{}'.format(company, sentiment_dict, vol_min, sent_min)
    df = pd.read_csv(file_path, encoding="utf-8", index_col=0)
    df.index = df.index.astype(str)
    print(len(df))

    for bad_day in bad_days:
        try:
            df = df.drop(bad_day)
        except Exception as e:
            print(e)


    print(len(df))
    return df


def open_df_sent(company):
    bad_days = ['2018-01-04', '2018-01-03', '2018-01-02', '2018-01-01', '2018-01-25', '2018-01-26', '2018-01-27',
                '2018-02-02', '2018-02-03', '2018-02-06', '2018-02-10', '2018-02-18']

    file_path = 'C:\\Users\\Open Account\\Documents\\BA_JonasIls\\Twitter_Streaming\\Sentiment_Dataframes\\20180101_20180217\\20180101_20180217_SentimentDataframes_{}'.format(company)
    df = pd.read_csv(file_path, encoding="utf-8")
    df['date'] = df['date'].astype(str)
    df = df.set_index('date')

    for bad_day in bad_days:
        try:
            df = df.drop(bad_day)
        except Exception as e:
            print(e)

    df = df.reset_index()
    df['date'] = pd.to_datetime(df['date'], errors='coerce')

    return df


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
        df_tweets = open_df_sent(company)
        raw_data.append(df_tweets)

    df_tweets = pd.concat(raw_data)

    df_tweets['date'] = pd.to_datetime(df_tweets['date'])
    df_tweets['time_adj'] = pd.to_datetime(df_tweets['time_adj'])
    df_tweets['time_adj'] = df_tweets['time_adj']
    df_tweets['hour'] = df_tweets['time_adj'].dt.hour
    df_tweets['time_adj'] = [timestamp.time() for timestamp in df_tweets['time_adj']]
    df_tweets['cw'] = dftweets.date.dt.week()
    df_tweets['weekday_number'] = df_tweets['date'].dt.dayofweek
    df_tweets['weekday_name'] = [calendar.day_name[day] for day in df_tweets['weekday_number']]
    df_tweets = df_tweets.reset_index()
    df_tweets = df_tweets.groupby(['weekday_number']).count()
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


def avg_tweetcount_weekday(company):
    df = open_df_sent(company)
    df = df[['date', 'time_adj']]
    print(df)
    df.date = pd.to_datetime(df.date)
    df = df.groupby('date').count()
    df = df.reset_index()
    df['dow'] = df['date'].dt.dayofweek
    df['cw'] = df['date'].dt.dayofweek
    df = df.groupby('dow').mean()
    df = df.reset_index()

    df.time_adj.plot(kind='bar', legend=False, color='black')
    #plt.bar(x, height, color=(0.2, 0.4, 0.6, 0.6))
    plt.xlabel('Weekday')
    plt.ylabel('Number of Tweets')
    plt.title('Average Number of Tweets per Weekday')
    plt.xticks([0, 1, 2, 3, 4, 5, 6], ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun'])

    plt.show()


def avg_tweetcount_hour(company, trading_day):
    df = open_df_sent(company)
    df = df[['date', 'time_adj']]
    #print(df)
    df.date = pd.to_datetime(df.date)
    df.time_adj = pd.to_datetime(df.time_adj)
    df['dow'] = df['date'].dt.dayofweek

    if trading_day == True:
        df = df.loc[df.dow.isin(range(0, 5))]

    else:
        df = df.loc[df.dow.isin([5, 6])]

    #df['hour'] = df['time_adj'].dt.hour
    time_int = df.time_adj.apply(time_to_int)
    df['hour'] = time_int.apply(get_trading_hour)

    df = df.groupby(['date', 'hour']).count()
    df = df.groupby('hour').mean()

    df.time_adj.plot(kind='bar', legend=False, color='black')
    plt.xlabel('Hour of the Day')
    plt.ylabel('Number of Tweets')
    plt.title('Average Number of Tweets per Hour')
    #plt.xticks(list(range(0, 24)),
    #           ['00:00 - 00:59', '01:00 - 01:59', '02:00 - 02:59', '03:00 - 03:59', '04:00 - 04:59', '05:00 - 05:59',
    #            '06:00 - 06:59', '07:00 - 07:59', '08:00 - 08:59', '09:00 - 09:59', '10:00 - 10:59', '11:00 - 11:59',
    #            '12:00 - 12:59', '13:00 - 13:59', '14:00 - 14:59', '15:00 - 15:59', '16:00 - 16:59', '17:00 - 17:59',
    #            '18:00 - 18:59', '19:00 - 19:59', '20:00 - 20:59', '21:00 - 21:59', '22:00 - 22:59', '23:00 - 23:59'])



    plt.show()


def plot_tweetcount_byweekday(company):
    df_tweets = open_df_sent(company)
    df_tweets = df_tweets[['date', 'time_adj']]
    print('opened')
    df_tweets['time_adj'] = pd.to_datetime(df_tweets['time_adj'], errors='coerce')
    df_tweets = df_tweets.loc[df_tweets.time_adj.notnull()]
    df_tweets['cw'] = dftweets.date.dt.week()
    df_tweets['hour'] = df_tweets['time_adj'].dt.hour
    #df_tweets['time_adj'] = [timestamp.time() for timestamp in df_tweets['time_adj']]
    df_tweets['dow'] = df_tweets['date'].dt.dayofweek
    #df_tweets = df_tweets.loc[df_tweets['dow'].isin(list(range(0, 5)))]
    print('time formated')

    weeks = df_tweets.cw.unique()

    for week in weeks:
        rows = df_tweets.loc[df_tweets.cw == week]
        days = df_tweets.dow.unique()
        #for day in days:



    # sum of counts for all combinations
    df_tweets = df_tweets.reset_index()
    df_tweets = df_tweets.groupby('dow').count()
    #df_tweets = df_tweets.groupby(['date', 'hour']).count()
    #df_tweets = df_tweets.reset_index().groupby(['hour']).mean()
    #df_tweets = df_tweets.groupby(['date', 'hour']).count()
    print('created means')

    #bins = df_tweets['hour'].unique().sort()
    df_tweets.plot(kind='bar', legend=False)
    plt.xlabel('Hour')
    plt.ylabel('Number of Tweets')
    plt.title('Tweets per Hour on Weekdays')
    #plt.title('Tweets per Hour on Weekends')
    #plt.xticks(list(range(0, 24)), ['00:00 - 00:59', '01:00 - 01:59', '02:00 - 02:59', '03:00 - 03:59', '04:00 - 04:59', '05:00 - 05:59', '06:00 - 06:59', '07:00 - 07:59', '08:00 - 08:59', '09:00 - 09:59', '10:00 - 10:59', '11:00 - 11:59', '12:00 - 12:59', '13:00 - 13:59', '14:00 - 14:59', '15:00 - 15:59', '16:00 - 16:59', '17:00 - 17:59', '18:00 - 18:59', '19:00 - 19:59', '20:00 - 20:59', '21:00 - 21:59', '22:00 - 22:59', '23:00 - 23:59'])
    plt.xticks([0, 1, 2, 3, 4, 5, 6], ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun'])

    #plt.savefig('C:\\Users\\Open Account\\Documents\\BA_JonasIls\\Literatur & Analysen\\Plots\\PlotTweetCount_Hour_Weekend', bbox_inches='tight')
    plt.show()


def plot_twittervsstock(company, sentiment_dict, stock_var, twi_var, vol_min, sent_min):
    #df_sentstock=df_sentstock.reset_index()
    #df_sentstock=df_sentstock.set_index('date')
    df = open_df_c2c(company, sentiment_dict, vol_min, sent_min)

    var_std = ['tweet_count', 'count_pos', 'count_neg', 'tweet_count_w', 'count_pos_w', 'count_neg_w']
    for x in var_std:
        mean = df['{}'.format(x)].mean()
        std = df['{}'.format(x)].std()
        df['{}_std'.format(x)] = ((df['{}'.format(x)] - mean) / std)

    df['Volume in USD'] = df['Volume'] * ((df['High'] + df['Low'])/2)

    df.index = df.index.astype(str)
    fig, ax1 = plt.subplots()
    ax2 = ax1.twinx()
    df['{}'.format(twi_var)].plot(kind='bar', color='y', ax=ax1)
    df['{}'.format(stock_var)].plot(kind='line', marker='d', ax=ax2)
    ax1.yaxis.tick_right()
    ax2.yaxis.tick_left()
    ax1.set_ylabel('{}'.format(twi_var), labelpad=30)
    ax2.set_ylabel('{}'.format(stock_var), labelpad=30)
    plt.xlabel('Trading Day')
    plt.title('{}: {} vs {}'.format(company, twi_var, stock_var))

    handles1, labels1 = ax1.get_legend_handles_labels()
    handles2, labels2 = ax2.get_legend_handles_labels()
    handles = handles1 + handles2
    labels = labels1 + labels2

    plt.legend(handles, labels,loc=0)

    plt.savefig('C:\\Users\\Open Account\\Documents\\BA_JonasIls\\Literatur & Analysen\\Plots\\Twitter_vs_Stock\\PlotTwiStock{}_{}_{}'.format(company, stock_var, twi_var), bbox_inches='tight')


def scatterplot(company, stock_var, twi_var, sentiment_dict, vol_min, sent_min):
    df = open_df_c2c(company, sentiment_dict, vol_min, sent_min)
    df.index = df.index.astype(str)
    print(len(df.index.unique()))

    var_std = ['tweet_count', 'count_pos', 'count_neg', 'tweet_count_w', 'count_pos_w', 'count_neg_w']
    for x in var_std:
        mean = df['{}'.format(x)].mean()
        std = df['{}'.format(x)].std()
        df['{}_std'.format(x)] = ((df['{}'.format(x)] - mean) / std)



    df = df[['{}'.format(stock_var), '{}'.format(twi_var)]]

    sns.regplot(x="{}".format(twi_var), y="{}".format(stock_var), data=df)
    plt.ylabel('{}'.format(stock_var))
    plt.xlabel('{}'.format(twi_var))
    plt.title('{} vs. {}'.format(stock_var, twi_var))
    #plt.savefig('C:\\Users\\Open Account\\Documents\\BA_JonasIls\\Literatur & Analysen\\Plots\\ScatterPlots\\PlotScatter{}_{}_{}'.format(company, stock_var, twi_var), bbox_inches='tight')

    plt.show()


LM = 'SentimentLM'
GI = 'SentimentGI'
HE = 'SentimentHE'
TB = 'SentimentTB'


# Plot Twitter vs Stock
stock_var = 'abnormal_returns'
twi_var = ['sent_mean_w', 'bullishness']

#plot_tweetcount_byweekday(companies)
#plot_twittervsstock_bydate(company='HD', sentiment_dict=GI, stock_var=stock_var, twi_var=twi_var)

companies = ['$MSFT', '$MMM', '$AXP', '$AAPL', '$BA', '$CAT', '$CVX', '$CSCO', '$KO', '$DWDP', '$DIS', '$XOM', '$GE', '$GS', '$HD', '$IBM', '$INTC', '$JNJ', '$JPM', '$MCD', '$MRK', '$NKE', '$PFE', '$PG', '$TRV', '$UTX', '$UNH', '$VZ', '$V', '$WMT']
companies = [company.replace('$', '') for company in companies]

#plot_tweetcount_byweekday('AllStocks')
#plot_tweetcount_bytradingday(companies)

#plot_tweetcount_byweekday(companies)

#for company in companies:
    #plt.figure(company)
    #print(company)
    #for var in twi_var:
        #plot_twittervsstock(company, GI, stock_var, var,  vol_min=0, sent_min=0)
#    scatterplot(company, sentiment_dict=GI, stock_var=stock_var, twi_var=twi_var, vol_min=0, sent_min=0)

def dont_know():
    for var in twi_var:
        plt.figure(var)
        dfs = []
        for company in companies:
            df = open_df_c2c(company, HE, 0, 0)
            df['close_std'] = (df['Adj Close'] - df['Adj Close'].mean()) / df['Adj Close'].std()
            dfs.append(df)

        df = pd.concat(dfs)
        df.index = df.index.astype(str)
        print(len(df.index.unique()))

        var_std = ['tweet_count', 'count_pos', 'count_neg', 'tweet_count_w', 'count_pos_w', 'count_neg_w']
        for x in var_std:
            mean = df['{}'.format(x)].mean()
            std = df['{}'.format(x)].std()
            df['{}_std'.format(x)] = ((df['{}'.format(x)] - mean) / std)

        df = df[['{}'.format(stock_var), '{}'.format(var)]]

        sns.regplot(x="{}".format(var), y="{}".format(stock_var), data=df)
        plt.ylabel('{}'.format(stock_var))
        plt.xlabel('{}'.format(var))
        plt.title('{} vs. {}'.format(stock_var, var))
        # plt.savefig('C:\\Users\\Open Account\\Documents\\BA_JonasIls\\Literatur & Analysen\\Plots\\ScatterPlots\\PlotScatter{}_{}_{}'.format(company, stock_var, twi_var), bbox_inches='tight')

        plt.show()


#avg_tweetcount_hour('AllStocks', trading_day=True)

df = open_df_sent('AllStocks')
print(len(df))