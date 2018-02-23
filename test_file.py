import pandas as pd
from textblob import TextBlob
import numpy as np

def get_TBSentiment(text):
    '''Function to calculate a sentiment score based on the textblob library'''
    analysis = TextBlob(text)
    SentimentTB = analysis.sentiment.polarity
    return(SentimentTB)


def open_df_sent(company):
    #file_path = 'C:\\Users\\Open Account\\Documents\\BA_JonasIls\\Twitter_Streaming\\Sentiment_Dataframes\\20180101\\20180101Test_SentimentsLM_{}.csv'.format(company)
    file_path = 'C:\\Users\\Open Account\\Documents\\BA_JonasIls\\Twitter_Streaming\\Sentiment_Dataframes\\20180124\\24012018Test_Sentiments_{}.csv'.format(company)
    df_tweets = pd.read_csv(file_path, encoding="utf-8")
    df_tweets = df_tweets.fillna(0)
    df_tweets = df_tweets.loc[df_tweets['user_followers'] not in [NaN, 'False', 'NaN']]
    #df_tweets['text_clean'] = df_tweets['text_clean'].astype(str)
    df_tweets['SentimentTB'] = df_tweets['text_clean'].apply(get_TBSentiment)

    return df_tweets

#msft = open_df_sent('MSFT')
#print(msft.user_followers)

def open_newsfeed(file_path):
    return(pd.read_csv(file_path, encoding="utf-8"))

def write_articles(file_path, df):
    return(df.to_csv(file_path, encoding='utf-8'))

def clean_text(text):
   return text

# einheitlich für positiv und negatives sentiment
def threshold_sentiment(df_sent, sent_dict, percentile):

    values_pos = []
    values_neg = []

    for index, tweet in df_sent.iterrows():
        sent = tweet['{}'.format(sent_dict)]

        # transforming negative sentiments
        if sent < 0:
           values_neg.append(sent)
        else:
            values_pos.append(sent)

    try:
        sent_min_pos = np.percentile(values_pos, percentile)
    except:
        sent_min_pos = 0
    try:
        sent_min_neg = np.percentile(values_neg, percentile)
    except:
        sent_min_neg = 0


    values = []
    for index, tweet in df_sent.iterrows():
        sent = tweet['{}'.format(sent_dict)]

        # transforming negative sentiments
        if sent < 0:
            sent = sent * (-1)
        else:
            sent = sent

        values.append(sent)

    try:
        sent_min = np.percentile(values, percentile)
    except:
        sent_min = 0