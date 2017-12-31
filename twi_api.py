
######## CONTENT ########

# 1. TWITTER
#    1.1. Twitter API
#    1.2. Analyzing sentiment per Tweet
#    1.3. Collecting Tweets
#    1.4. Creating timeseries of daily sentiment
# 2. STOCK DATA
# 3. CORRELATION



# __________________________________________________________________
# 1. TWITTER
# __________________________________________________________________


# __________________________________________________________________
# 1.1 Twitter API
# __________________________________________________________________
print("Twitter Crawler")

import datetime
import re

import pandas as pd
import tweepy
from pandas_datareader.data import DataReader
from pytz import timezone
from textblob import TextBlob
from tweepy import OAuthHandler
#from textblob import TextBlob
from pytz import timezone
import pandas as pd
from pandas_datareader.data import DataReader


# Class constructor or initialization method.
class TwitterClient(object):
    # Class constructor or initialization method.
    def __init__(self):
        consumer_key = "xPdDjbhEHyFMzCW90KGAummxz"
        consumer_secret = "zValY66MwanWf7XOqBNLFGqZsBpjq84Qo6jHddFOwhLnxSIOyJ"
        access_token = "912230531089223680-9TlKB13hYMTkbJpRpKHCDxuWX0ugtUI"
        access_secret = "nRAMXKPi33IO9esWebcjVikeBBF2XOzXyJ3ADD6kvaBIe"

        # attempt authentication
        try:
            # create OAuthHandler object
            self.auth = OAuthHandler(consumer_key, consumer_secret)
            # set access token and secret
            self.auth.set_access_token(access_token, access_secret)
            # create tweepy API object to fetch tweets
            self.api = tweepy.API(self.auth)
        except:
            print("Error: Authentication Failed")

# __________________________________________________________________
#  1.2 Analyzing Sentiment per Tweet
# __________________________________________________________________

    def __clean_tweet__(self, tweet):
        '''
        Utility function to clean tweet text by removing links, special characters
        using simple regex statements.
        '''
        return ' '.join(re.sub("(@[A-Za-z0-9]+)|([^0-9A-Za-z \t])|(\w+:\/\/\S+)", " ", tweet).split())

    def __get_tweet_sentiment__(self, tweet):
        '''
        Utility function to classify sentiment of passed tweet
        using textblob's sentiment method
        '''
        # create TextBlob object of passed tweet text
        analysis = TextBlob(self.__clean_tweet__(tweet))
        # set sentiment
        if analysis.sentiment.polarity > 0:
            return 'positive'
        elif analysis.sentiment.polarity == 0:
            return 'neutral'
        else:
            return 'negative'

# __________________________________________________________________
# 1.3 Collecting Tweets
# __________________________________________________________________

    def get_tweets(self, query, count):
        '''
        Main function to fetch tweets and parse them.
        '''
        # empty list to store parsed tweets
        tweets = []

        try:
            # call twitter api to fetch tweets
            fetched_tweets = self.api.search(q=query, count=count)

            # parsing tweets one by one
            for tweet in fetched_tweets:
                # empty dictionary to store required params of a tweet
                parsed_tweet = {}

                tweet_timestamp = tweet.created_at
                print(tweet_timestamp)


                # adjusting timestamp to EST
                EST = timezone('EST')
                fmt = '%Y-%m-%d %H:%M:%S'
                adjusting = tweet_timestamp.astimezone(EST).strftime(fmt)
                timestamp_adjusted = datetime.datetime.strptime(adjusting, fmt)

                # converting timestamp to integer to exclude tweets outside the relevant time range
                def to_integer(ts):
                    return 100 * ts.hour + ts.minute


                time_int = to_integer(timestamp_adjusted.time())

                # saving text of tweet
                parsed_tweet['text'] = tweet.text

                # saving sentiment
                #parsed_tweet['sentiment'] = self.__get_tweet_sentiment__(tweet.text)

                # saving timestamp
                parsed_tweet['datetime_adjusted'] = timestamp_adjusted
                parsed_tweet['time_int'] = time_int
                parsed_tweet['entities'] = tweet.entities

                print(parsed_tweet['entities'])

                # exlcluding irrelevant tweets
                #   1. excluding tweets outside the relevant time range
                if parsed_tweet['time_int'] > 930 and parsed_tweet['time_int'] < 1600:

                #   2. excluding retweets
                    if tweet.retweet_count > 0:
                        # if tweet has retweets, ensure that it is appended only once
                        if parsed_tweet not in tweets:
                            tweets.append(parsed_tweet)
                    else:
                        tweets.append(parsed_tweet)

            return tweets

        except tweepy.TweepError as e:
            print("Error : " + str(e))

# __________________________________________________________________
# 1.4 Creating timeseries of daily sentiment
# __________________________________________________________________

def daily_sentiment(date_start, date_end):

    # creating object of TwitterClient Class
    api = TwitterClient()

    # calling function to get tweets
    tweets = api.get_tweets(query="$AMZN", since=date_start, until=date_end)

    # empty dictionary to save percentage of positive tweets
    sentiments = {}
    # picking positive tweets from tweets
    ptweets = [tweet for tweet in tweets if tweet['sentiment'] == 'positive']
    # percentage of positive tweets
    sentiments['sentiment_positive'] = format(100 * len(ptweets) / len(tweets))

    # picking negative tweets from tweets
    ntweets = [tweet for tweet in tweets if tweet['sentiment'] == 'negative']
    # percentage of negative tweets
    sentiments['sentiment_negative'] = format(100 * len(ntweets) / len(tweets))

    # defining date of sentiment analyis
    sentiments['date'] = date_start

    return sentiments

def timeseries_sentiment():

    # soring daily sentiments in list
    sentiments_ts = [daily_sentiment(date_start=datetime.date(2017, 10, 25), date_end=datetime.date(2017, 10, 26)),
                     daily_sentiment(date_start=datetime.date(2017, 10, 26), date_end=datetime.date(2017, 10, 27)),
                     daily_sentiment(date_start=datetime.date(2017, 10, 27), date_end=datetime.date(2017, 10, 28)),
                     #daily_sentiment(date_start= datetime.date(2017, 10, 28) , date_end= datetime.date(2017, 10, 29)),
                     #daily_sentiment(date_start= datetime.date(2017, 10, 29) , date_end= datetime.date(2017, 10, 30)),
                     #daily_sentiment(date_start= datetime.date(2017, 10, 30) , date_end= datetime.date(2017, 10, 31)),
                     #daily_sentiment(date_start= datetime.date(2017, 10, 31) , date_end= datetime.date(2017, 11, 1))
                     ]

    # storing results in dataframe
    df = pd.DataFrame(sentiments_ts)
    df_sentiments = df.set_index('date')

    #sentiments_file = sentiments_file.to_csv('sentiments_appl.csv')

    return df_sentiments #, sentiments_file

#__________________________________________________________________
# 2. STOCK DATA
#__________________________________________________________________


def stockdata():

    start = '2017-10-25'
    end = '2017-10-27'

    # collecting stockdata
    stocks = DataReader("AMZN", 'yahoo', start, end)['Adj Close']

    # calculating yields
    yields = stocks/stocks.shift(1)-1
    df_yields = yields.drop(yields.index[0])

    print(stocks)

    #stockdata_file = stockdata.to_csv('stockdata_aapl.csv')

    return df_yields#,stockdata_file

#__________________________________________________________________
# 3. CORRELATION
#__________________________________________________________________

import numpy as np

def correlation():
    """ Main Function to get corr between Sentiment and Price Data"""
    # calling stockdata
    df_yields = stockdata()
    # calling sentiments
    df_sentiments = timeseries_sentiment()

    # merging dataframes
    df_results = pd.concat([df_yields, df_sentiments], axis=1, ignore_index=True)

    # changing data type of columns
    df_results[0] = np.float64(df_results[0])
    df_results[1] = np.float64(df_results[1])
    df_results[2] = np.float64(df_results[2])

    # renaming columns
    df_results.rename(columns={0: 'return_t_t-1', 1: 'Negative', 2: 'Positive'}, inplace=True)

    #df_results.rename(columns={'Adj Close': 'return_t_t-1', 'sentiment_negative': 'negative', 'sentiment_positiv':'positive'}, inplace=True)

    print(df_results)

    # calculating correlation between stocks and sentiments
    correlation_negative = df_results['return_t_t-1'].corr(df_results['Negative'])
    correlation_positive = df_results['return_t_t-1'].corr(df_results['Positive'])

    print("Corr Negative Sentiment")
    print(correlation_negative)

    print("Corr Positive Sentiment")
    print(correlation_positive)

    return correlation

def main():
    # creating object of TwitterClient Class
    api = TwitterClient()
    # calling function to get tweets
    api.get_tweets(query='$MSFT', count=200)


if __name__ == '__main__':
    main()
