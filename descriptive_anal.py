import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from textblob import TextBlob
import calendar


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

    bins = [-1.0, -0.8, -0.6, -0.4, -0.2, 0.0, 0.2, 0.4, 0.6, 0.8, 1.0]
    plt.hist(sent_list, bins, histtype='bar', rwidth=0.6)
    plt.xlabel('{}'.format(sentiment_dict))
    plt.ylabel('Number of Tweets')
    plt.title('Histogram of Tweets Sentiment')
    plt.show()

def plot_tweetcount_bytime(list_of_companies):
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



LM = 'SentimentLM'
GI = 'SentimentGI'
HE = 'SentimentHE'
TB = 'SentimentTB'

# Define companies you'd like to analyze
companies = ['$MSFT', '$MMM', '$AXP', '$AAPL', '$BA', '$CAT', '$CVX', '$CSCO', '$KO', '$DWDP', '$DIS', '$XOM', '$GE', '$GS', '$HD', '$IBM', '$INTC', '$JNJ', '$JPM', '$MCD', '$MRK', '$NKE', '$PFE', '$PG', '$TRV', '$UTX', '$UNH', '$VZ', '$V', '$WMT']
companies = [company.replace('$', '') for company in companies]

#plot_sentiment_dist(companies, TB)
plot_tweetcount_bytime(companies)

