import pandas as pd
import numpy as np
from decimal import Decimal
from textblob import TextBlob

#tweets_data_path = 'C:\\Users\\Open Account\\Documents\\BA_JonasIls\\Twitter_Streaming\\Excel\\twittertext_MSFT.xls'
#tweets_file = pd.read_excel(tweets_data_path, index_label='date', encoding="utf-8")

#tweets_file.to_csv('C:\\Users\\Open Account\\Documents\\BA_JonasIls\\Twitter_Streaming\\Excel\\twittertext_MSFT.csv')
#print(tweets_file)

# reading dataframe
def open_df_tweets(file_path):
    return pd.read_csv(file_path, encoding="utf-8")

def get_TBSentiment(text):
    analysis = TextBlob(text)
    SentimentTB = analysis.sentiment.polarity

    return(SentimentTB)

def dict_evaluation(dataframe, dict, sent_min, rel_min):

    dataframe['SentimentTB'] = dataframe['text_clean'].apply(get_TBSentiment)

    TR = 0  # Signifikantes Sentiment und tatsaechlich relevant
    FR = 0  # Signifikantes Sentiment aber tatsaechlich irrelevant
    FI = 0  # Insignifikantes Sentiment aber tatsaechlich relevant
    TI = 0  # Insignifikantes Sentiment und tatsaechlich irrelevant

    nothing = 0 # if no statement return True

    FI_tweets = []
    TR_tweets = []

    # iterating Statements over Dataframe to count
    for index, tweet in dataframe.iterrows():


        sent = tweet['{}'.format(dict)]
        rel = tweet['sent_score']

        # transforming negative sentiments to simplify if-Statments
        if sent < 0:
            sent = sent * (-1)
        else:
            sent = sent

        #print("SENT TYPE")
        #print(type(sent))
        #print(type(0.2))

        #print("REL TYPE")
        #print(type(rel))
        #print(type(3))

        # classifying tweets
        if sent >= sent_min and rel >= rel_min:
            TR = TR + 1
            TR_tweets.append(tweet)
        elif sent >= sent_min and rel < rel_min:
            FR = FR + 1
        elif sent < sent_min and rel >= rel_min:
            FI = FI + 1
            FI_tweets.append(tweet)

        elif sent < sent_min and rel < rel_min:
            TI = TI + 1
        else:
            nothing = nothing + 1

    df_FI = pd.DataFrame(FI_tweets)
    df_FI.to_csv('C:\\Users\\Open Account\\Documents\\BA_JonasIls\\Evaluation Dicts\\FI_tweets{}.csv'.format(dict))

    df_TR = pd.DataFrame(TR_tweets)
    df_TR.to_csv('C:\\Users\\Open Account\\Documents\\BA_JonasIls\\Evaluation Dicts\\TR_tweets{}.csv'.format(dict))

    #print(nothing)

    # creating matrix to store metrics
    ev_dict = {}
    ev_dict['sigificant_{}'.format(dict)] = [TR, FR]
    ev_dict['insignificant_{}'.format(dict)] = [FI, TI]

    ev_matrix = pd.DataFrame.from_dict(ev_dict, orient='index')
    ev_matrix['Sum'] = ev_matrix[0]+ev_matrix[1]
    ev_matrix.rename(columns={0: 'relevant', 1: 'irrelevant'}, inplace=True)

    return(ev_matrix)

def ev_metrics(sent_dicts, sent_min, file_path):
    df_tweets = open_df_tweets(file_path)
    analyzed_dicts = []

    for sent_dict in sent_dicts:
        ev_matrix = dict_evaluation(df_tweets, sent_dict, sent_min, 2)

        metrics = {}
        metrics['sent_dict'] = sent_dict
        metrics['precision'] = ev_matrix.loc['sigificant_{}'.format(sent_dict)]['relevant'] / ev_matrix.loc['sigificant_{}'.format(sent_dict)]['Sum']
        metrics['recall'] = ev_matrix.loc['sigificant_{}'.format(sent_dict)]['relevant'] / ev_matrix['relevant'].sum()
        metrics['f1_measure'] = 2 * ((metrics['precision'] * metrics['recall']) / (metrics['precision'] + metrics['recall']))

        analyzed_dicts.append(metrics)

    return pd.DataFrame(analyzed_dicts).set_index('sent_dict')

if __name__ == "__main__":
    LM = 'SentimentLM'
    GI = 'SentimentGI'
    HE = 'SentimentHE'
    QDAP = 'SentimentQDAP'
    TB = 'SentimentTB'
    sent_dicts = [LM, GI, HE, QDAP, TB]

    file_path = 'C:\\Users\\Open Account\\Documents\\BA_JonasIls\\Evaluation Dicts\\ev_dicts.csv'
    #df_tweets = open_df_tweets(file_path)

    df_metrics = ev_metrics(sent_dicts, 0.2, file_path)
    df_metrics.to_excel('C:\\Users\\Open Account\\Documents\\BA_JonasIls\\Evaluation Dicts\\ev_metrics.xls')
    print(df_metrics)

    #for sent_dict in sent_dicts:
    #    ev_matrix = dict_evaluation(df_tweets, sent_dict, 0.2, 3)
    #    ev_matrix.to_excel('C:\\Users\\Open Account\\Documents\\BA_JonasIls\\Evaluation Dicts\\Evaluation Matrix {}.xls'.format(sent_dict))