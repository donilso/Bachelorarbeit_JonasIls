import re
import tweepy
from tweepy import OAuthHandler
from textblob import TextBlob
import pytz
import datetime

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

                tweettimestamp = tweet.created_at
                timestamp_converted = tweettimestamp.strftime("%m, %d, %Y, %H, %M")
                tweettime = datetime.datetime.fromtimestamp(int(timestamp_converted)).strftime('%H:%M')

                # saving text of tweet
                parsed_tweet['text'] = tweet.text
                # saving sentiment of tweet
                parsed_tweet['datetime'] = tweettimestamp
                parsed_tweet['time'] = tweettime

            return tweets

        except tweepy.TweepError as e:
            print("Error : " + str(e))

def main():

    api = TwitterClient()
    tweets = api.get_tweets(query ="$Appl", count = 10)

    for tweet in tweets:
        print (tweet.text)
        print (tweet['time'])

main()

print ("finished Test File")
