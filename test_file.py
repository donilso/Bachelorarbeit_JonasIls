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

def open_evdicts(file_path):
    return(pd.read_csv(file_path, encoding="utf-8"))


def get_texts(file_path, rel_score):
    df_tweets = open_evdicts(file_path)
    rows = df_tweets.loc[df_tweets['sent_score'] == rel_score]
    rows['text_clean'].to_excel('C:\\Users\\Open Account\\Documents\\BA_JonasIls\\Evaluation Dicts\\ev_dicts_texts_{}.xls'.format(rel_score), encoding='utf-8')
    return(rows['text_clean'])

if __name__ == "__main__":
    file_path = 'C:\\Users\\Open Account\\Documents\\BA_JonasIls\\Evaluation Dicts\\ev_dicts.csv'
    print(get_texts(file_path, 3))
    print(get_texts(file_path, 2))
