import re
import tweepy
from tweepy import OAuthHandler
from textblob import TextBlob
import datetime

import pytz
from pytz import timezone
import pandas as pd

print ("Start Test File")

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



                #adjusting the datetime to timezone
                EST = timezone('EST')
                fmt = '%Y-%m-%d %H:%M:%S'

                timestamp_adjusted = tweet_timestamp.astimezone(EST).strftime(fmt)
                tweet_timestamp_adjusted = datetime.datetime.strptime(timestamp_adjusted, fmt)


                #converting timestamp to integer
                def to_integer(ts):
                    return 100 * ts.hour + ts.minute

                time_int = to_integer(tweet_timestamp_adjusted.time())

                # saving text of tweet
                parsed_tweet['text'] = tweet.text

                #saving timestamp
                parsed_tweet['datetime'] = tweet_timestamp
                parsed_tweet['datetime_adjusted'] = tweet_timestamp_adjusted
                parsed_tweet['time'] = tweet_timestamp_adjusted.time()
                parsed_tweet['time_int'] = time_int
                parsed_tweet['date']= tweet_timestamp_adjusted.date()

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

#______________________________________________________________
#______________________________________________________________


def main():

    api = TwitterClient()
    tweets = api.get_tweets(query ="$Appl", count = 1000)

    dataframe = pd.DataFrame(tweets)
    tweets_dataframe = dataframe.drop(['text', 'date', 'datetime', 'time', 'time_int'], axis=1)

        #print(tweet['time'])
        #print(tweet['time_int'])
    print(tweets_dataframe)

        #print(tweet['date'])
        #print(tweet['text'])
        #print(tweet['datetime'])
        #print(tweet['datetime_adjusted'])

main()


print ("finished Test File")
