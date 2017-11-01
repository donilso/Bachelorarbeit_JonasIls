
#TODO Tweets of the last week

print("hello world")
print("hallo jonas")

# --------------------------
# 1. Twitter API
# --------------------------

import datetime
import re
import tweepy
from tweepy import OAuthHandler
from textblob import TextBlob
from pytz import timezone
import pandas as pd


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
        analysis = TextBlob(self.clean_tweet(tweet))
        # set sentiment
        if analysis.sentiment.polarity > 0:
            return 'positive'
        elif analysis.sentiment.polarity == 0:
            return 'neutral'
        else:
            return 'negative'



    def get_tweets(self, query, since, until ):
        '''
        Main function to fetch tweets and parse them.
        '''
        # empty list to store parsed tweets
        tweets = []

        try:
            # call twitter api to fetch tweets
            fetched_tweets = self.api.search(q=query, since=since, until= until)

            # parsing tweets one by one
            for tweet in fetched_tweets:
                # empty dictionary to store required params of a tweet
                parsed_tweet = {}

                tweet_timestamp = tweet.created_at

                # adjusting the datetime to timezone
                EST = timezone('EST')
                fmt = '%Y-%m-%d %H:%M:%S'
                timestamp_adjusted = tweet_timestamp.astimezone(EST).strftime(fmt)
                tweet_timestamp_adjusted = datetime.datetime.strptime(timestamp_adjusted, fmt)

                # converting timestamp to integer
                def to_integer(ts):
                    return 100 * ts.hour + ts.minute

                time_int = to_integer(tweet_timestamp_adjusted.time())

                # saving text of tweet
                parsed_tweet['text'] = tweet.text

                # saving sentiment
                parsed_tweet['sentiment'] = self.__get_tweet_sentiment__(tweet.text)

                # saving timestamp
                parsed_tweet['datetime'] = tweet_timestamp
                parsed_tweet['datetime_adjusted'] = tweet_timestamp_adjusted
                parsed_tweet['time'] = tweet_timestamp_adjusted.time()
                parsed_tweet['date'] = tweet_timestamp_adjusted.date()
                parsed_tweet['time_int'] = time_int

                if parsed_tweet['time_int'] > 930 and parsed_tweet['time_int'] < 1600:

                    if tweet.retweet_count > 0:

                        # if tweet has retweets, ensure that it is appended only once
                        if parsed_tweet not in tweets:
                            tweets.append(parsed_tweet)
                    else:
                        tweets.append(parsed_tweet)

            return tweets

        except tweepy.TweepError as e:
            print("Error : " + str(e))


# ----------------------
# 3. Main Function
# ----------------------
def main():
    # creating object of TwitterClient Class
    api = TwitterClient()
    # calling function to get tweets
    tweets = api.get_tweets(query=query, count=count)
    # picking positive tweets from tweets
    ptweets = [tweet for tweet in tweets if tweet['sentiment'] == 'positive']
    # percentage of positive tweets
    sentiment_positive = "Positive tweets percentage: {} %".format(100 * len(ptweets) / len(tweets))

    # picking negative tweets from tweets
    ntweets = [tweet for tweet in tweets if tweet['sentiment'] == 'negative']
    # percentage of negative tweets
    sentiment_negative = "Negative tweets percentage: {} %".format(100 * len(ntweets) / len(tweets))

    # picking neutral tweets from tweets
    neutweets = [tweet for tweet in tweets if tweet ['sentiment'] == 'neutral']
    # percentage of neutral tweets
    sentiment_neutral = "Neutral tweets percentage: {} % \
        ".format(100 * (len(tweets) - len(ntweets) - len(ptweets)) / len(tweets))

    # sentiment as a list of percentages
    sentiment_overview = [sentiment_positive, sentiment_negative, sentiment_neutral]
    print(sentiment_overview)

    # printing positive tweets
    #print("\n\nPositive tweets:")
    #for tweet in ptweets:
    #    print(tweet['text'])
    #    print(tweet['datetime_adjusted'])

    # printing negative tweets
    #print("\n\nNegative tweets:")
    #for tweet in ntweets:
    #    print(tweet['text'])
    #    print(tweet['datetime_adjusted'])

    # printing neutral tweets
    #print("\n\nNeutral tweets")
    #for tweet in neutweets:
    #    print(tweet['text'])
    #    print(tweet['datetime_adjusted'])

    print("Number of analyzed Tweets:")
    print(len(tweets))

    print(tweets_dataframe)

    # saving results in csv-file
#
#    file = open(r'/Users/Jonas/Desktop/BA_Results/APPL_results.csv', 'w')

#    file.write('Sentiment_Overview \n\n')
#    file.write(str(sentiment_overview))

#    file.write('\n\n\n\n positive tweets \n\n')
#    file.write(str(ptweets))

#    file.write('\n\n\n\n\n\n Negative Tweets \n\n')
#    file.write(str(ntweets))

#    file.write('\n\n\n\n\n\n\n\n Neutral Tweets \n\n')
#    file.write(str(neutweets))

#    file.close()
