class tweet():
    def __init__(self):
        self.text = ""
        self.datetime = ""
        self.author = "USERID"
        self.sentiment=""


class tweetManager():
    def __init__(self):
        self.tweets = []

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
        print (tweet.created_at)

    def __get_tweet_user__(self, tweet):
        tweet.author = API.get_user(id)

