import pandas as pd
from textblob import TextBlob
import numpy as np
import re
import json


def get_TBSentiment(text):
    '''Function to calculate a sentiment score based on the textblob library'''
    analysis = TextBlob(text)
    SentimentTB = analysis.sentiment.polarity
    return(SentimentTB)

def open_newsfeed(file_path):
    return(pd.read_csv(file_path, encoding="utf-8"))

def write_articles(file_path, df):
    return(df.to_csv(file_path, encoding='utf-8'))


def drop_lines(dataframe):
    df = dataframe.loc('Error accessing: ' not in dataframe['article'])
    return df


def text_from_bits(bits, encoding='utf-8', errors='surrogatepass'):
    n = int(bits, 2)
    return n.to_bytes((n.bit_length() + 7) // 8, 'big').decode(encoding, errors) or '\0'


def clean_text(content):

    url_str = r'http[s]?://(?:[a-z]|[0-9]|[$-_@.&amp;+]|[!*\(\),]|(?:%[0-9a-f][0-9a-f]))+'
    html_str = r'<[^>]+>'
    non_ascii_str = r'[^\x00-\x7f]'
    disclosure_str = r'(additional\s)?disclosure:\s[^"]+$'

    regex_remove = [url_str, html_str, disclosure_str, non_ascii_str]

    regex_str = [
        r'(?:(?:\d+,?)+(?:\.?\d+)?)',  # numbers
        r"(?:[a-z][a-z'\-_]+[a-z])",  # words with - and '
        r'(?:[\w_]+)',  # other words
        r'(?:\S)'  # anything else
    ]

    tokens_re = re.compile(r'(' + '|'.join(regex_str) + ')', re.VERBOSE | re.IGNORECASE)


    def text_from_bits(bits, encoding='utf-8', errors='surrogatepass'):
        n = int(bits, 2)
        return n.to_bytes((n.bit_length() + 7) // 8, 'big').decode(encoding, errors) or '\0'

    def remove(content):
        ch_toreplace = ['#', '\n', '\r']
        content = content.replace(''.join(ch_toreplace), ' ')
        content =  re.sub(r'(' + '|'.join(regex_remove) + ')', '', content, re.VERBOSE | re.IGNORECASE)
        return(content)

    def tokenize(text):
        return tokens_re.findall(text)

    def preprocess(content, lowercase=False):
        text = remove(content)
        #tokens = tokenize(text)
        #if lowercase:
        #    tokens = [token if emoticon_re.search(token) else token.lower() for token in tokens]
        return text

    return preprocess(content)

#df_news = pd.read_csv('C:\\Users\\Open Account\\Documents\\BA_JonasIls\\Newsfeeds\\Cleaned_Newsfeed_$AXP.csv', sep='#')
numbers = [1, 2, 3, 4, 5, 6]
dfs = list()
for n in numbers:
    df = pd.read_csv('C:\\Users\\Open Account\\Documents\\BA_JonasIls\\Twitter_Streaming\\Sentiment_Dataframes\\20180101_20180217\\dfsent_AAPL{}'.format(n), sep=',', encoding='utf-8')
    dfs.append(df)

df = pd.concat(dfs)
df.to_csv('C:\\Users\\Open Account\\Documents\\BA_JonasIls\\Twitter_Streaming\\Sentiment_Dataframes\\20180101_20180217\\20180101_20180217_SentimentDataframes_AAPL', sep=',', encoding='utf-8')

companies = ['$MSFT', '$MMM', '$AAPL',  '$AXP', '$BA', '$CAT', '$CVX', '$CSCO', '$KO', '$DWDP', '$DIS', '$XOM', '$GE', '$GS', '$HD', '$IBM', '$INTC', '$JNJ', '$JPM', '$MCD', '$MRK', '$NKE', '$PFE', '$PG', '$TRV', '$UTX', '$UNH', '$VZ', '$V', '$WMT']
companies = [company.replace('$', '') for company in companies]

dfs = list()
for company in companies:
    print(company)
    df = pd.read_csv('C:\\Users\\Open Account\\Documents\\BA_JonasIls\\Twitter_Streaming\\Sentiment_Dataframes\\20180101_20180217\\20180101_20180217_SentimentDataframes_{}'.format(company), sep=',', encoding='utf-8')
    df = df[['date', 'id', 'text_clean', 'time_adj', 'user_followers', 'retweet', 'timeslot', 'SentimentLM', 'SentimentGI']]
    print('Get TB...')
    df['SentimentTB'] = df['text_clean'].apply(get_TBSentiment)
    df.to_csv('C:\\Users\\Open Account\\Documents\\BA_JonasIls\\Twitter_Streaming\\Sentiment_Dataframes\\20180101_20180217\\20180101_20180217_SentimentDataframes_{}'.format(company),sep=',', encoding='utf-8')
    dfs.append(df)

df = pd.concat(dfs)
df.to_csv('C:\\Users\\Open Account\\Documents\\BA_JonasIls\\Twitter_Streaming\\Sentiment_Dataframes\\20180101_20180217\\20180101_20180217_DfSent_AllStocks', sep=',', encoding='utf-8')