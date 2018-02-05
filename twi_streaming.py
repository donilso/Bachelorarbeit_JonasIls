import tweepy
from tweepy import Stream
from tweepy import OAuthHandler
from tweepy.streaming import StreamListener
import boto3
from contextlib import contextmanager
import tempfile

##################################
### TWITTER STREAM ###############
##################################

consumer_key = "xPdDjbhEHyFMzCW90KGAummxz"
consumer_secret = "zValY66MwanWf7XOqBNLFGqZsBpjq84Qo6jHddFOwhLnxSIOyJ"
access_token = "912230531089223680-9TlKB13hYMTkbJpRpKHCDxuWX0ugtUI"
access_secret = "nRAMXKPi33IO9esWebcjVikeBBF2XOzXyJ3ADD6kvaBIe"


#This is a basic listener that just prints received tweets to stdout.
class MyListener(StreamListener):

    def on_data(self, data):
        try:
            with open('twitter_streaming.json', 'a') as f:
                f.write(data)

            print(data)
            return True
        except BaseException as e:
            print("Error on_data: %s" % str(e))
        return True

    def on_error(self, status):
        print(status)
        return True

##################################
### S3 ACCESS ####################
##################################

if __name__ == "__main__":

    #This handles Twitter authetification and the connection to Twitter Streaming API
    l = MyListener()
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.secure = True
    auth.set_access_token(access_token, access_secret)

    data = stream.filter(track=['$MSFT', '$MMM', '$AXP', '$AAPL', '$BA', '$CAT', '$CVX', '$CSCO', '$KO', '$DWDP', '$DIS', '$XOM', '$GE', '$GS', '$HD', '$IBM', '$INTC', '$JNJ', '$JPM', '$MCD', '$MRK', '$NKE', '$PFE', '$PG', '$TRV', '$UTX', '$UNH', '$VZ', '$V', '$WMT'],
                         languages=['en'])
    stream = Stream(auth, l)


