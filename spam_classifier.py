import pandas as pd
from textblob import TextBlob
from textblob.classifiers import NaiveBayesClassifier

df = pd.read_csv("C:\\Users\\Open Account\\Documents\\BA_JonasIls\\Twitter_Streaming\\Sentiment_Dataframes\\20180101_20180217\\20180101_20180217_SentimentDataframes_AllStocks",
                   encoding="utf-8")
df = df.sample(n=1250)
df.to_excel("C:\\Users\\Open Account\\Documents\\BA_JonasIls\\Twitter_Streaming\\Sentiment_Dataframes\\20180101_20180217\\Spam_Clean\\spamclassifier.xls",
               encoding="utf-8",
               index_label="date")

def tain_spamclassifier(filepath):
    df = pd.read_excel(filepath, encoding="utf-8")
    df = df[['text_clean', 'spam']]
    tc = len(df) * 0.8
    nc = len(df) - tc

    train = df[0:tc-1]
    test = df[tc:nc-1]

    cl = NaiveBayesCalssifier(train)

    return cl