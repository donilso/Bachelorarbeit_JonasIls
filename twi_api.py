print("hello world")
print("hallo jonas")

# --------------------------
# 1. Twitter API
# --------------------------

import re
import tweepy
from tweepy import OAuthHandler
from textblob import TextBlob


# Class constructor or initialization method.
class TwitterClient(object):
    # Class constructor or initialization method.
    def __init__(self):
        consumer_key = "8Ca3VZdZ8FAHBQHY5Q4JqQJ95"
        consumer_secret = "jAooexmkwG8QxW8ODcK8FqNB5ZLVqaoqA5lgfINqucJYMuNoJE"
        access_token = "912230531089223680-0UiOqekdECbqtKtHaPF2mnZVejrgmb5"
        access_secret = "SCGpxSkF5jwszN59etpVRKs4B9oi5wbCFysQIW1MgDmMD"

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

        # ---------------------------
        # 2. Tweets sammeln und aufbereiten
        # ---------------------------

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

    def __get_tweet_datetime__(self, tweet):
        '''
        Utility function to get datetime for tweet
        '''
        tweet = tweets[0]
        print(tweet.created_at)

    def get_tweets(self, query, count=10):
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

                # saving text of tweet
                parsed_tweet['text'] = tweet.text
                # saving sentiment of tweet
                parsed_tweet['sentiment'] = self.__get_tweet_sentiment__(tweet.text)
                # saving datetime of tweet
                parsed_tweet['datetime'] = self.__get_tweet_datetime__


                # appending parsed tweet to tweets list
                if tweet.retweet_count > 0:
                    # if tweet has retweets, ensure that it is appended only once
                    if parsed_tweet not in tweets:
                        tweets.append(parsed_tweet)
                else:
                    tweets.append(parsed_tweet)

            # return parsed tweets
            return tweets

        except tweepy.TweepError as e:
            # print error (if any)
            print("Error : " + str(e))


# ----------------------
# 3. Main Function
# ----------------------
def main():
    # creating object of TwitterClient Class
    api = TwitterClient()
    # calling function to get tweets
    tweets = api.get_tweets(query="$Appl", count=100)
    # picking positive tweets from tweets
    ptweets = [tweet for tweet in tweets if tweet['sentiment'] == 'positive']
    # percentage of positive tweets
    print("Positive tweets percentage: {} %".format(100 * len(ptweets) / len(tweets)))
    # picking negative tweets from tweets
    ntweets = [tweet for tweet in tweets if tweet['sentiment'] == 'negative']
    # percentage of negative tweets
    print("Negative tweets percentage: {} %".format(100 * len(ntweets) / len(tweets)))
    # percentage of neutral tweets
    print("Neutral tweets percentage: {} % \
        ".format(100 * (len(tweets) - len(ntweets) - len(ptweets)) / len(tweets)))
    # todo use stock tickers

    # printing first 5 positive tweets
    print("\n\nPositive tweets:")
    for tweet in ptweets[:10]:
        print(tweet['text', 'datetime'])

    # printing first 5 negative tweets
    print("\n\nNegative tweets:")
    for tweet in ntweets[:10]:
        print(tweet['text', 'datetime'])