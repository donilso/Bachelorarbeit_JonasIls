import datetime
import re
import tweepy
from tweepy import OAuthHandler
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
        callback_url = "https://github.com/donilso/Bachelorarbeit_JonasIls"

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

    def word_in_text(self, text):
        keywords = ["stock", "price", "market", "share"]

        for keyword in keywords:

            keyword = keyword.lower()
            text = text.lower()
            match = re.search(keyword, text)
            if match:
                return True
            return False

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

                # empty dictionary to story required params of tweet
                parsed_tweet = {}

                # fetch text and language
                parsed_tweet['text'] = tweet.text
                parsed_tweet['lang'] = tweet.lang

                # analyszing date & time
                timestamp = tweet.created_at

                # adjusting timestamp to EST
                EST = timezone('EST')
                fmt = '%Y-%m-%d %H:%M:%S'
                adjusting = timestamp.astimezone(EST).strftime(fmt)
                timestamp_adj = datetime.datetime.strptime(adjusting, fmt)
                parsed_tweet['time_adj'] = timestamp_adj.time()
                parsed_tweet['time_fetched'] = timestamp.time()
                parsed_tweet['date'] = timestamp_adj.date()

                # converting timestamp to integer to classify tweets by "timeslot"
                def to_integer(ts):
                    return 100 * ts.hour + ts.minute

                time_int = to_integer(timestamp_adj.time())
                #
                if time_int > 930 and time_int < 1600:
                    parsed_tweet["timeslot"] = "during"
                else:
                    if time_int > 0 and time_int < 930:
                        parsed_tweet["timeslot"] = "before"
                    if time_int > 1600 and time_int < 2359:
                        parsed_tweet["timeslot"] = "after"

                # analyzing popularity
                parsed_tweet['retweets'] = tweet.retweet_count
                parsed_tweet['favorite'] = tweet.favorite_count

                user_data = tweet.user
                parsed_tweet['user_name'] = user_data.name
                parsed_tweet['user_follower'] = user_data.followers_count

                # analyzing relevance
                parsed_tweet['relevant'] = self.word_in_text(parsed_tweet['text'])

                # exlcluding retweets
                if tweet.retweet_count > 0:
                    # if tweet has retweets, ensure that it is appended only once
                    if parsed_tweet not in tweets:
                        tweets.append(parsed_tweet)
                else:
                    tweets.append(parsed_tweet)

            df_tweets = pd.DataFrame(tweets).set_index('date')

            return df_tweets
        except tweepy.TweepError as e:
            print("Error : " + str(e))

def main():
        # creating object of TwitterClient Class
        api = TwitterClient()
        # calling function to get tweets
        tweets = api.get_tweets(query='$GE', count=200)
        print(tweets)


if __name__ == '__main__':
    main()