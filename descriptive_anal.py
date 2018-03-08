import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from textblob import TextBlob
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
    #df_sentstock.to_csv('C:\\Users\\Open Account\\Documents\\BA_JonasIls\\Twitter_Streaming\\Sentiment_Dataframes\\20180101_20180217\\C2C_Dataframes\\20180101_20180217_C2C_{}'.format(company), index_label='date')
    return(df_sentstock)


def time_to_int(ts):
    return 10000 * ts.hour + 100 * ts.minute + ts.second


def get_TBSentiment(text):
    '''Function to calculate a sentiment score based on the textblob library'''
    analysis = TextBlob(text)
    SentimentTB = analysis.sentiment.polarity
    return(SentimentTB)


def open_df_c2c(company):
    #file_path = 'C:\\Users\\Open Account\\Documents\\BA_JonasIls\\Twitter_Streaming\\Sentiment_Dataframes\\20180101\\20180101Test_SentimentsLM_{}.csv'.format(company)
    file_path = 'C:\\Users\\Open Account\\Documents\\BA_JonasIls\\Twitter_Streaming\\Sentiment_Dataframes\\20180101_20180217\\C2C_Dataframes\\20180101_20180217_C2C_{}'.format(company)
    return pd.read_csv(file_path, encoding="utf-8", parse_dates=True)


def open_df_sent(company):
    file_path = 'C:\\Users\\Open Account\\Documents\\BA_JonasIls\\Twitter_Streaming\\Sentiment_Dataframes\\20180101_20180217\\20180101_20180217_SentimentDataframes_{}'.format(company)
    df_tweets = pd.read_csv(file_path, encoding="utf-8")
    df_tweets['date'] = pd.to_datetime(df_tweets['date'])
    #df_tweets['SentimentTB'] = df_tweets['text_clean'].apply(get_TBSentiment)

    return df_tweets


def plot_sentiment_dist(list_of_companies, sentiment_dict):
    raw_data = []
    for company in list_of_companies:
        print('Open {}'.format(company))
        df_tweets = open_df_sent(company)
        raw_data.append(df_tweets)

    sent_list = pd.concat(raw_data)
    sent_list = sent_list['{}'.format(sentiment_dict)]
    print(len(sent_list))

    bins = [-1.0, -0.8, -0.6, -0.4, -0.2, 0.0, 0.2, 0.4, 0.6, 0.8, 1.0]

    plt.hist(sent_list, bins, histtype='bar', alpha=0.8, rwidth=0.8)
    plt.xlabel('{}'.format(sentiment_dict))
    plt.ylabel('Number of Tweets')
    plt.title('Histogram of Tweets Sentiment')

    plt.savefig('C:\\Users\\Open Account\\Documents\\BA_JonasIls\\Literatur & Analysen\\Plots\\Sent_Distribution\\PlotSentDist_{}'.format(sentiment_dict), bbox_inches='tight')


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


def plot_twittervsstock(company, df_sentstock, stock_var, twi_var):
    df_sentstock.index = df_sentstock.index.astype(str)
    fig, ax1 = plt.subplots()
    ax2 = ax1.twinx()
    df_sentstock['{}'.format(twi_var)].plot(kind='bar', color='y', ax=ax1)
    df_sentstock['{}'.format(stock_var)].plot(kind='line', marker='d', ax=ax2)
    ax1.yaxis.tick_right()
    ax2.yaxis.tick_left()
    plt.xlabel('Trading Day')
    plt.title('Tweets per Weekday')

    plt.savefig('C:\\Users\\Open Account\\Documents\\BA_JonasIls\\Literatur & Analysen\\Plots\\Twitter_vs_Stock\\PlotTwiStock{}_{}_{}'.format(company, stock_var, twi_var), bbox_inches='tight')


def scatterplot(company, df_sentstock, stock_var, twi_var):
    df_sentstock.index = df_sentstock.index.astype(str)
    #plt.scatter(df_sentstock['{}'.format(stock_var)], df_sentstock['{}'.format(twi_var)])

    df_sentstock.plot(x=stock_var, y=twi_var, style='o')
    plt.xlabel('{}'.format(stock_var))
    plt.ylabel('{}'.format(twi_var))
    plt.title('{} vs. {}'.format(stock_var, twi_var))
    plt.savefig('C:\\Users\\Open Account\\Documents\\BA_JonasIls\\Literatur & Analysen\\Plots\\ScatterPlots\\PlotScatter{}_{}_{}'.format(company, stock_var, twi_var), bbox_inches='tight')

LM = 'SentimentLM'
GI = 'SentimentGI'
HE = 'SentimentHE'
TB = 'SentimentTB'


# Plot Twitter vs Stock
stock_var = 'volatility_parks'
twi_var = 'sent_std'

#plot_tweetcount_byweekday(companies)
#plot_twittervsstock_bydate(company='HD', sentiment_dict=GI, stock_var=stock_var, twi_var=twi_var)

companies = ['$MSFT', '$MMM', '$AXP', '$BA', '$CAT', '$CVX', '$CSCO', '$KO', '$DWDP', '$DIS', '$XOM', '$GE', '$GS', '$HD', '$IBM', '$INTC', '$JNJ', '$JPM', '$MCD', '$MRK', '$NKE', '$PFE', '$PG', '$TRV', '$UTX', '$UNH', '$VZ', '$V', '$WMT']
companies = [company.replace('$', '') for company in companies]

#plot_sentiment_dist(companies, TB)

plot_tweetcount_byweekday(companies)

#plot_tweetcount_byweekday(companies)

#for company in companies:
    #print(company)
    #df_c2c = open_df_c2c(company)
    #scatterplot(company, df_c2c, stock_var, twi_var)
    #plot_twittervsstock(company, df_c2c, stock_var, twi_var)

df_c2cAllStocks = pd.read_csv('C:\\Users\\Open Account\\Documents\\BA_JonasIls\\Literatur & Analysen\\Correlations\\All_Stocks\\SentimentGI\\20180217_DF_C2CSentimentGI_50vol_50sen.csv', encoding='utf-8')
scatterplot('AllStocks', df_c2cAllStocks, stock_var, twi_var)

