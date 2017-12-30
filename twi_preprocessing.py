import tweepy
from tweepy import Stream
from tweepy import OAuthHandler
from tweepy.streaming import StreamListener
import time
import string
import json
import pandas as pd
import json
import matplotlib.pyplot as plt
import re
from pytz import timezone

class RSSFeeds(object):

    # Creating List of instances to make class iterable
    _by_company = []

    # Declaring attributes of instances
    def __init__(self, url_Feed, company_Feed):
        self.url_Feed = url_Feed
        self._by_company.append(self)

        self.company_Feed = company_Feed

MSFT = RSSFeeds (url_Feed=['https://finance.google.com/finance/company_news?q=NASDAQ:MSFT&ei=Ya0tWomUMIugswGAqLrwDg&output=rss',
                          'http://finance.yahoo.com/rss/headline?s=MSFT'],
                company_Feed='$MSFT')
MMM = RSSFeeds(url_Feed = ['https://finance.google.com/finance/company_news?q=NYSE:MMM&ei=aKotWrHONIOIsAGXqouYCg&output=rss',
                           'http://finance.yahoo.com/rss/headline?s=MMM'],
               company_Feed='$MMM')
AXP = RSSFeeds(url_Feed = ['https://finance.google.com/finance/company_news?q=NYSE:AXP&ei=oKotWvnGBc2EsQGD5IeQDQ&output=rss',
                           'http://finance.yahoo.com/rss/headline?s=AXP'],
               company_Feed='$AXP')
AAPL = RSSFeeds(url_Feed = ['https://finance.google.com/finance/company_news?q=NASDAQ:AAPL&ei=tqotWsCCFJDDsAGv8pCACg&output=rss',
                            'http://finance.yahoo.com/rss/headline?s=AAPL'],
                company_Feed='$AAPL')
BA = RSSFeeds(url_Feed = ['https://finance.google.com/finance/company_news?q=NYSE:BA&ei=yKotWqiBF82HsQHk2onwBA&output=rss',
                          'http://finance.yahoo.com/rss/headline?s=BA'],
              company_Feed='$BA')
CAT = RSSFeeds(url_Feed = ['https://finance.google.com/finance/company_news?q=NYSE:CAT&ei=2KotWrkXhvizAc6DhbgD&output=rss',
                           'http://finance.yahoo.com/rss/headline?s=CAT'],
               company_Feed='$CAT')
CVX = RSSFeeds(url_Feed = ['https://finance.google.com/finance/company_news?q=NYSE:CVX&ei=6qotWqi_LIHuUbqjkeAC&output=rss',
                           'http://finance.yahoo.com/rss/headline?s=CVX)'],
               company_Feed='$CVX')
CSCO = RSSFeeds(url_Feed = ['https://finance.google.com/finance/company_news?q=NASDAQ:CSCO&ei=EKstWrCcG8zisgH3zaqQCA&output=rss',
                            'http://finance.yahoo.com/rss/headline?s=CSCO'],
                company_Feed='$CSCO')
KO = RSSFeeds(url_Feed = ['https://finance.google.com/finance/company_news?q=NYSE:KO&ei=H6stWrmlJY_AswH0rYD4BA&output=rss',
                          'http://finance.yahoo.com/rss/headline?s=KO'],
              company_Feed='$KO')
DWDP = RSSFeeds(url_Feed = ['https://finance.google.com/finance/company_news?q=NYSE:DWDP&ei=MqstWpmtGNuCswGs0bnoCA&output=rss',
                            'http://finance.yahoo.com/rss/headline?s=DWDP'],
                company_Feed='$DWDP')
DIS = RSSFeeds(url_Feed = ['https://finance.google.com/finance/company_news?q=NYSE:DIS&ei=Q6stWvDRHtKFsQH6yYa4Ag&output=rss',
                           'http://finance.yahoo.com/rss/headline?s=DIS)'],
               company_Feed='$DIS')
XOM = RSSFeeds(url_Feed = ['https://finance.google.com/finance/company_news?q=NYSE:XOM&ei=UqstWsGFE9LHsAGem4WYBQ&output=rss',
                           'http://finance.yahoo.com/rss/headline?s=XOM'],
               company_Feed='$XOM')
GE = RSSFeeds(url_Feed = ['https://finance.google.com/finance/company_news?q=NYSE:GE&ei=YqstWoDnMZDDsAGv8pCACg&output=rss',
                          'http://finance.yahoo.com/rss/headline?s=GE'],
              company_Feed='$GE')
GS = RSSFeeds(url_Feed = ['https://finance.google.com/finance/company_news?q=NYSE:GS&ei=dKstWpChD9uCswGs0bnoCA&output=rss',
                          'http://finance.yahoo.com/rss/headline?s=GS'],
              company_Feed='$GS')
HD = RSSFeeds(url_Feed = ['https://finance.google.com/finance/company_news?q=NYSE:HD&ei=pKstWsCWN9SHsAGx7ozwDQ&output=rss',
                          'http://finance.yahoo.com/rss/headline?s=HD'],
              company_Feed='$HD')
IBM = RSSFeeds(url_Feed = ['https://finance.google.com/finance/company_news?q=NYSE:IBM&ei=t6stWuCVGMzisgH3zaqQCA&output=rss',
                           'http://finance.yahoo.com/rss/headline?s=IBM'],
               company_Feed='$IBM')
INTC = RSSFeeds(url_Feed = ['https://finance.google.com/finance/company_news?q=NASDAQ:INTC&ei=2qstWtDBGYeLsQGTq5nwCw&output=rss',
                            'http://finance.yahoo.com/rss/headline?s=INTC'],
                company_Feed='$INTC')
JNJ = RSSFeeds(url_Feed = ['https://finance.google.com/finance/company_news?q=NYSE:JNJ&ei=6qstWsDbIIb4swHOg4W4Aw&output=rss',
                           'http://finance.yahoo.com/rss/headline?s=JNJ'],
               company_Feed='$JNJ')
JPM = RSSFeeds(url_Feed = ['https://finance.google.com/finance/company_news?q=NYSE:JPM&ei=_astWvCtKtWvsgHC5Y2QCQ&output=rss',
                           'http://finance.yahoo.com/rss/headline?s=JPM'],
               company_Feed='$JPM')
MCD = RSSFeeds(url_Feed = ['https://finance.google.com/finance/company_news?q=NYSE:MCD&ei=FqwtWsnkKougswGAqLrwDg&output=rss',
                           'http://finance.yahoo.com/rss/headline?s=MCD'],
               company_Feed='$MCD')
MRK = RSSFeeds(url_Feed = ['https://finance.google.com/finance/company_news?q=NYSE:MRK&ei=JawtWqCSLtuCswGs0bnoCA&output=rss',
                           'http://finance.yahoo.com/rss/headline?s=MRK'],
               company_Feed='$MRK')
NKE = RSSFeeds(url_Feed = ['https://finance.google.com/finance/company_news?q=NYSE:NKE&ei=NqwtWpmGJZLKswGZmYTIBg&output=rss',
                           'http://finance.yahoo.com/rss/headline?s=NKE'],
               company_Feed='$NKE')
PFE = RSSFeeds(url_Feed = ['https://finance.google.com/finance/company_news?q=NYSE:PFE&ei=Q6wtWsC4ApDEsAGNiaKwDw&output=rss',
                           'http://finance.yahoo.com/rss/headline?s=PFE'],
               company_Feed='$PFE')
PG = RSSFeeds(url_Feed=['https://finance.google.com/finance/company_news?q=NYSE:PG&ei=WKwtWuC_Co7DswHlyZD4CA&output=rss',
                        'http://finance.yahoo.com/rss/headline?s=PG'],
              company_Feed='$PG')
TRV = RSSFeeds(url_Feed=['https://finance.google.com/finance/company_news?q=NYSE:TRV&ei=t6wtWsCmCNOQswHv2rTgBQ&output=rss',
                         'http://finance.yahoo.com/rss/headline?s=TRV'],
               company_Feed='$TRV')
UTX = RSSFeeds(url_Feed=['https://finance.google.com/finance/company_news?q=NYSE:UTX&ei=5KwtWpD9GofaswGx_J6QBA&output=rss',
                         'http://finance.yahoo.com/rss/headline?s=UTX'],
               company_Feed='$UTX')
UNH = RSSFeeds(url_Feed=['https://finance.google.com/finance/company_news?q=NYSE:UNH&ei=9KwtWpDIAta_swHi-J5w&output=rss',
                         'http://finance.yahoo.com/rss/headline?s=UNH'],
               company_Feed='$UNH')
VZ = RSSFeeds(url_Feed=['https://finance.google.com/finance/company_news?q=NYSE:VZ&ei=Ca0tWvDCI8zesgG3w6KwAg&output=rss',
                        'http://finance.yahoo.com/rss/headline?s=VZ'],
              company_Feed='$VZ')
V = RSSFeeds(url_Feed=['https://finance.google.com/finance/company_news?q=NYSE:V&ei=Fq0tWumSJsbIU_CyofgB&output=rss',
                       'http://finance.yahoo.com/rss/headline?s=V'],
             company_Feed='V')
WMT = RSSFeeds(url_Feed=['https://finance.google.com/finance/company_news?q=NYSE:WMT&ei=Mq0tWtD8L4KCsQH5jb3gCA&output=rss',
                         'http://finance.yahoo.com/rss/headline?s=WMT'],
               company_Feed='$WMT')

def word_in_text(text):
    keywords = ["stock", "price", "market", "share"]

    for keyword in keywords:

        keyword = keyword.lower()
        text = text.lower()
        match = re.search(keyword, text)
        if match:
            return True
        return False


def extract_link(text):
    regex = r'https?://[^\s<>"]+|www\.[^\s<>"]+'
    match = re.search(regex, text)
    if match:
        return match.group()
    return ''


def main():
    #for company in RSSFeeds._by_company:
        #Reading Tweets

        tweets_data_path ='C:\\Users\\Open Account\\Documents\\BA_JonasIls\\twitter_streaming.json'

        tweets_data = []
        tweets_file = open(tweets_data_path, "r")
        for line in tweets_file:
            try:
                tweet = json.loads(line)
                tweets_data.append(tweet)
            except:
                continue

        #Structuring Tweets
        analyzed_tweets = []

        for tweet in tweets_data:
            parsed_tweet = {}

            parsed_tweet['text'] =tweet['text']
            parsed_tweet['lang'] =tweet['lang']
            timestamp = tweet['created_at']

            # adjusting timestamp to EST
            EST = timezone('EST')
            fmt = '%Y-%m-%d %H:%M:%S'
            adjusting = timestamp.astimezone(EST).strftime(fmt)
            timestamp_adj = datetime.datetime.strptime(adjusting, fmt)
            parsed_tweet['time_adj'] = timestamp_adj.time
            parsed_tweet['time_fetched'] = timestamp.time
            parse_tweets['date'] = timestamp_adj.date

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

            parsed_tweet['retweets'] = tweet['retweet_count']
            parsed_tweet['favorite'] = tweet['favorite_count']
            parsed_tweet['user'] = tweet['user']

            #analyzing relevance
            parsed_tweet['relevant'] = word_in_text(parsed_tweet['text'])

            #exlcluding retweets
            if tweet.retweet_count > 0:
                # if tweet has retweets, ensure that it is appended only once
                if parsed_tweet not in tweets:
                    analyzed_tweets.append(parsed_tweet)
            else:
                analyzed_tweets.append(parsed_tweet)

        df_tweets = pd.DataFrame(analyzed_tweets)
        #df_tweets.set_index('date')
        print(df_tweets)

if __name__=='__main__':
    main()