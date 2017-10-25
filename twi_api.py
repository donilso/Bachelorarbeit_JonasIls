print("hello world")
print("hallo jonas")

# --------------------------
# 1. Twitter API
# --------------------------

import pytz
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

    def get_tweets(self, query, count=10):
        '''
        Main function to fetch tweets and parse them.
        '''
        # empty list to store parsed tweets
        tweets = []
        tz = pytz.timezone('US/Central')

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
                parsed_tweet['datetime'] = tweet.created_at
                parsed_tweet['datetime_adjusted'] = tz.localize(tweet.created_at)

                # appending parsed tweet to tweets list
                if tweet.retweet_count > 0:

                    # if tweet has retweets, ensure that it is appended only once
                    if parsed_tweet not in tweets:
                        tweets.append(parsed_tweet)
                else:
                    tweets.append(parsed_tweet)

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
    tweets = api.get_tweets(query="$Appl", count=10000)
    # creating object time

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
    print("\n\nPositive tweets:")
    for tweet in ptweets:
        print(tweet['text'])
        print(tweet['datetime_adjusted'])

    # printing negative tweets
    print("\n\nNegative tweets:")
    for tweet in ntweets:
        print(tweet['text'])
        print(tweet['datetime_adjusted'])

    # printing neutral tweets
    print("\n\nNeutral tweets")
    for tweet in neutweets:
        print(tweet['text'])
        print(tweet['datetime_adjusted'])

    # saving results in csv-file

    file = open(r'/Users/Jonas/Desktop/BA_Results/APPL_results.csv', 'w')

    file.write('Sentiment_Overview \n\n')
    file.write(str(sentiment_overview))

    file.write('\n\n\n\n positive tweets \n\n')
    file.write(str(ptweets))

    file.write('\n\n\n\n\n\n Negative Tweets \n\n')
    file.write(str(ntweets))

    file.write('\n\n\n\n\n\n\n\n Neutral Tweets \n\n')
    file.write(str(neutweets))

    file.close()

main()