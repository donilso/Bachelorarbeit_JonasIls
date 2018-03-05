import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from textblob import TextBlob
import calendar
import sent_stock_corr


def time_to_int(timestamp):
    return 10000 * ts.hour + 100 * ts.minute + ts.second


def get_TBSentiment(text):
    '''Function to calculate a sentiment score based on the textblob library'''
    analysis = TextBlob(text)
    SentimentTB = analysis.sentiment.polarity
    return(SentimentTB)


def open_df_sent(company, relevant_data):
    #file_path = 'C:\\Users\\Open Account\\Documents\\BA_JonasIls\\Twitter_Streaming\\Sentiment_Dataframes\\20180101\\20180101Test_SentimentsLM_{}.csv'.format(company)
    file_path = 'C:\\Users\\Open Account\\Documents\\BA_JonasIls\\Twitter_Streaming\\Sentiment_Dataframes\\20180124\\24012018Test_Sentiments_clean_{}.csv'.format(company)
    df_tweets = pd.read_csv(file_path, encoding="utf-8")

    if relevant_data == TB:
        df_tweets['text_clean'] = df_tweets['text_clean'].astype(str)
        df_tweets['SentimentTB'] = df_tweets['text_clean'].apply(get_TBSentiment)

    return df_tweets[relevant_data]


def plot_sentiment_dist(list_of_companies, sentiment_dict):
    raw_data = []
    for company in list_of_companies:
        df_tweets = open_df_sent(company, sentiment_dict)
        raw_data.append(df_tweets)

    sent_list = pd.concat(raw_data)

    #bins = [-1.0, -0.8, -0.6, -0.4, -0.2, 0.0, 0.2, 0.4, 0.6, 0.8, 1.0]
    bins = 11
    plt.hist(sent_list, bins, histtype='bar', alpha=0.8, rwidth=0.8)
    plt.xlabel('{}'.format(sentiment_dict))
    plt.ylabel('Number of Tweets')
    plt.title('Histogram of Tweets Sentiment')
    plt.show()


def plot_tweetcount_byweekday(list_of_companies):
    raw_data = []
    for company in list_of_companies:
        df_tweets = open_df_sent(company, ['time_adj', 'date'])
        raw_data.append(df_tweets)

    df_tweets = pd.concat(raw_data)
    df_tweets['date'] = pd.to_datetime(df_tweets['date'])
    df_tweets['time_adj'] = pd.to_datetime(df_tweets['time_adj'])
    df_tweets['time_adj'] = [timestamp.time() for timestamp in df_tweets['time_adj']]
    df_tweets['time_int'] = df_tweets['time_adj'].apply(time_to_int)
    df_tweets['weekday_number'] = df_tweets['date'].dt.dayofweek
    df_tweets['weekday_name'] = [calendar.day_name[day] for day in df_tweets['weekday_number']]

    bins = df_tweets['weekday_number'].unique().sort()

    plt.hist(df_tweets['weekday_number'], bins, histtype='bar', rwidth=0.6)
    plt.xlabel('Weekday')
    plt.ylabel('Number of Tweets')
    plt.title('Tweets per Weekday')
    plt.show()


def plot_twittervsstock_bydate(company, stock_var, twi_var, sentiment_dict):

    df_tweets = sent_stock_corr.open_df_sent(company)

    start = sent_stock_corr.date_start(df_tweets)
    end = sent_stock_corr.date_end(df_tweets)

    df_stock = sent_stock_corr.daily_yield(company, end=end, start=start)
    df_stock = df_stock['{}'.format(stock_var)]

    df_tweets = sent_stock_corr.close2close_sentiments(df_sent=df_tweets, df_stock=df_stock,
                                                       sent_mins=0, vol_mins=0, sentiment_filter=False, volume_filter=False,
                                                       sent_dict=sentiment_dict)
    df_tweets = df_tweets['{}'.format(twi_var)]

    df_twistock = pd.concat([df_tweets, df_stock], axis=1)
    df_twistock.index = df_twistock.index.astype(str)
    #df_twistock = df_twistock.reset_index()
    print(df_twistock)

    fig, ax1 = plt.subplots()
    ax2 = ax1.twinx()
    df_twistock['{}'.format(twi_var)].plot(kind='bar', color='y', ax=ax1)
    df_twistock['{}'.format(stock_var)].plot(kind='line', marker='d', ax=ax2)
    ax1.yaxis.tick_right()
    ax2.yaxis.tick_left()

    plt.show()

LM = 'SentimentLM'
GI = 'SentimentGI'
HE = 'SentimentHE'
TB = 'SentimentTB'

# Define companies you'd like to analyze
companies = ['$MSFT', '$MMM', '$AXP', '$AAPL', '$BA', '$CAT', '$CVX', '$CSCO', '$KO', '$DWDP', '$DIS', '$XOM', '$GE', '$GS', '$HD', '$IBM', '$INTC', '$JNJ', '$JPM', '$MCD', '$MRK', '$NKE', '$PFE', '$PG', '$TRV', '$UTX', '$UNH', '$VZ', '$V', '$WMT']
companies = [company.replace('$', '') for company in companies]

#plot_sentiment_dist(companies, TB)
#plot_tweetcount_bytime(companies)

# Plot Twitter vs Stock
stock_var = 'abnormal_returns'
twi_var = 'tweet_count'

plot_twittervsstock_bydate(company='CAT', sentiment_dict=TB, stock_var=stock_var, twi_var=twi_var)
