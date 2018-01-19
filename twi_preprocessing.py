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
import datetime

class RSSFeeds(object):

    # Creating List of instances to make class iterable
    _by_company = []

    # Declaring attributes of instances
    def __init__(self, url_Feed, company_Feed, identifier):
        self.url_Feed = url_Feed
        self._by_company.append(self)
        self.company_Feed = company_Feed
        self.identifier = identifier

MSFT = RSSFeeds (url_Feed=['https://finance.google.com/finance/company_news?q=NASDAQ:MSFT&ei=Ya0tWomUMIugswGAqLrwDg&output=rss',
                          'http://finance.yahoo.com/rss/headline?s=MSFT'],
                company_Feed='$MSFT',
                identifier =['Microsoft', 'microsoft', 'MSFT'])
MMM = RSSFeeds(url_Feed = ['https://finance.google.com/finance/company_news?q=NYSE:MMM&ei=aKotWrHONIOIsAGXqouYCg&output=rss',
                           'http://finance.yahoo.com/rss/headline?s=MMM'],
               company_Feed='$MMM',
               identifier = ['3M','MMM', 'mmm'] )
AXP = RSSFeeds(url_Feed = ['https://finance.google.com/finance/company_news?q=NYSE:AXP&ei=oKotWvnGBc2EsQGD5IeQDQ&output=rss',
                           'http://finance.yahoo.com/rss/headline?s=AXP'],
               company_Feed='$AXP',
               identifier = ['American Express', 'AMEX', 'american express'] )
AAPL = RSSFeeds(url_Feed = ['https://finance.google.com/finance/company_news?q=NASDAQ:AAPL&ei=tqotWsCCFJDDsAGv8pCACg&output=rss',
                            'http://finance.yahoo.com/rss/headline?s=AAPL'],
                company_Feed='$AAPL',
                identifier = [])
BA = RSSFeeds(url_Feed = ['https://finance.google.com/finance/company_news?q=NYSE:BA&ei=yKotWqiBF82HsQHk2onwBA&output=rss',
                          'http://finance.yahoo.com/rss/headline?s=BA'],
              company_Feed='$BA',
                identifier = [])
CAT = RSSFeeds(url_Feed = ['https://finance.google.com/finance/company_news?q=NYSE:CAT&ei=2KotWrkXhvizAc6DhbgD&output=rss',
                           'http://finance.yahoo.com/rss/headline?s=CAT'],
               company_Feed='$CAT',
                identifier = [])
CVX = RSSFeeds(url_Feed = ['https://finance.google.com/finance/company_news?q=NYSE:CVX&ei=6qotWqi_LIHuUbqjkeAC&output=rss',
                           'http://finance.yahoo.com/rss/headline?s=CVX)'],
               company_Feed='$CVX',
                identifier = [])
CSCO = RSSFeeds(url_Feed = ['https://finance.google.com/finance/company_news?q=NASDAQ:CSCO&ei=EKstWrCcG8zisgH3zaqQCA&output=rss',
                            'http://finance.yahoo.com/rss/headline?s=CSCO'],
                company_Feed='$CSCO',
                identifier = [])
KO = RSSFeeds(url_Feed = ['https://finance.google.com/finance/company_news?q=NYSE:KO&ei=H6stWrmlJY_AswH0rYD4BA&output=rss',
                          'http://finance.yahoo.com/rss/headline?s=KO'],
              company_Feed='$KO',
                identifier = [])
DWDP = RSSFeeds(url_Feed = ['https://finance.google.com/finance/company_news?q=NYSE:DWDP&ei=MqstWpmtGNuCswGs0bnoCA&output=rss',
                            'http://finance.yahoo.com/rss/headline?s=DWDP'],
                company_Feed='$DWDP',
                identifier = [])
DIS = RSSFeeds(url_Feed = ['https://finance.google.com/finance/company_news?q=NYSE:DIS&ei=Q6stWvDRHtKFsQH6yYa4Ag&output=rss',
                           'http://finance.yahoo.com/rss/headline?s=DIS)'],
               company_Feed='$DIS',
                identifier = [])
XOM = RSSFeeds(url_Feed = ['https://finance.google.com/finance/company_news?q=NYSE:XOM&ei=UqstWsGFE9LHsAGem4WYBQ&output=rss',
                           'http://finance.yahoo.com/rss/headline?s=XOM'],
               company_Feed='$XOM',
                identifier = [])
GE = RSSFeeds(url_Feed = ['https://finance.google.com/finance/company_news?q=NYSE:GE&ei=YqstWoDnMZDDsAGv8pCACg&output=rss',
                          'http://finance.yahoo.com/rss/headline?s=GE'],
              company_Feed='$GE',
                identifier = [])
GS = RSSFeeds(url_Feed = ['https://finance.google.com/finance/company_news?q=NYSE:GS&ei=dKstWpChD9uCswGs0bnoCA&output=rss',
                          'http://finance.yahoo.com/rss/headline?s=GS'],
              company_Feed='$GS',
                identifier = [])
HD = RSSFeeds(url_Feed = ['https://finance.google.com/finance/company_news?q=NYSE:HD&ei=pKstWsCWN9SHsAGx7ozwDQ&output=rss',
                          'http://finance.yahoo.com/rss/headline?s=HD'],
              company_Feed='$HD',
                identifier = [])
IBM = RSSFeeds(url_Feed = ['https://finance.google.com/finance/company_news?q=NYSE:IBM&ei=t6stWuCVGMzisgH3zaqQCA&output=rss',
                           'http://finance.yahoo.com/rss/headline?s=IBM'],
               company_Feed='$IBM',
                identifier = [])
INTC = RSSFeeds(url_Feed = ['https://finance.google.com/finance/company_news?q=NASDAQ:INTC&ei=2qstWtDBGYeLsQGTq5nwCw&output=rss',
                            'http://finance.yahoo.com/rss/headline?s=INTC'],
                company_Feed='$INTC',
                identifier = [])
JNJ = RSSFeeds(url_Feed = ['https://finance.google.com/finance/company_news?q=NYSE:JNJ&ei=6qstWsDbIIb4swHOg4W4Aw&output=rss',
                           'http://finance.yahoo.com/rss/headline?s=JNJ'],
               company_Feed='$JNJ',
                identifier = [])
JPM = RSSFeeds(url_Feed = ['https://finance.google.com/finance/company_news?q=NYSE:JPM&ei=_astWvCtKtWvsgHC5Y2QCQ&output=rss',
                           'http://finance.yahoo.com/rss/headline?s=JPM'],
               company_Feed='$JPM',
                identifier = [])
MCD = RSSFeeds(url_Feed = ['https://finance.google.com/finance/company_news?q=NYSE:MCD&ei=FqwtWsnkKougswGAqLrwDg&output=rss',
                           'http://finance.yahoo.com/rss/headline?s=MCD'],
               company_Feed='$MCD',
                identifier = [])
MRK = RSSFeeds(url_Feed = ['https://finance.google.com/finance/company_news?q=NYSE:MRK&ei=JawtWqCSLtuCswGs0bnoCA&output=rss',
                           'http://finance.yahoo.com/rss/headline?s=MRK'],
               company_Feed='$MRK',
                identifier = [])
NKE = RSSFeeds(url_Feed = ['https://finance.google.com/finance/company_news?q=NYSE:NKE&ei=NqwtWpmGJZLKswGZmYTIBg&output=rss',
                           'http://finance.yahoo.com/rss/headline?s=NKE'],
               company_Feed='$NKE',
                identifier = [])
PFE = RSSFeeds(url_Feed = ['https://finance.google.com/finance/company_news?q=NYSE:PFE&ei=Q6wtWsC4ApDEsAGNiaKwDw&output=rss',
                           'http://finance.yahoo.com/rss/headline?s=PFE'],
               company_Feed='$PFE',
                identifier = [])
PG = RSSFeeds(url_Feed=['https://finance.google.com/finance/company_news?q=NYSE:PG&ei=WKwtWuC_Co7DswHlyZD4CA&output=rss',
                        'http://finance.yahoo.com/rss/headline?s=PG'],
              company_Feed='$PG',
                identifier = [])
TRV = RSSFeeds(url_Feed=['https://finance.google.com/finance/company_news?q=NYSE:TRV&ei=t6wtWsCmCNOQswHv2rTgBQ&output=rss',
                         'http://finance.yahoo.com/rss/headline?s=TRV'],
               company_Feed='$TRV',
                identifier = [])
UTX = RSSFeeds(url_Feed=['https://finance.google.com/finance/company_news?q=NYSE:UTX&ei=5KwtWpD9GofaswGx_J6QBA&output=rss',
                         'http://finance.yahoo.com/rss/headline?s=UTX'],
               company_Feed='$UTX',
                identifier = [])
UNH = RSSFeeds(url_Feed=['https://finance.google.com/finance/company_news?q=NYSE:UNH&ei=9KwtWpDIAta_swHi-J5w&output=rss',
                         'http://finance.yahoo.com/rss/headline?s=UNH'],
               company_Feed='$UNH',
                identifier = [])
VZ = RSSFeeds(url_Feed=['https://finance.google.com/finance/company_news?q=NYSE:VZ&ei=Ca0tWvDCI8zesgG3w6KwAg&output=rss',
                        'http://finance.yahoo.com/rss/headline?s=VZ'],
              company_Feed='$VZ',
                identifier = [])
V = RSSFeeds(url_Feed=['https://finance.google.com/finance/company_news?q=NYSE:V&ei=Fq0tWumSJsbIU_CyofgB&output=rss',
                       'http://finance.yahoo.com/rss/headline?s=V'],
             company_Feed='$V',
                identifier = [])
WMT = RSSFeeds(url_Feed=['https://finance.google.com/finance/company_news?q=NYSE:WMT&ei=Mq0tWtD8L4KCsQH5jb3gCA&output=rss',
                         'http://finance.yahoo.com/rss/headline?s=WMT'],
               company_Feed='$WMT',
                identifier = [])


def word_in_text(text):
    keywords = ["stock", "price", "market", "share"]

    for keyword in keywords:

        keyword = keyword.lower()
        text = text.lower()
        match = re.search(keyword, text)
        if match:
            return True
        return False

def clean_text(content):
    emoticons_str = r"""
        (?:
            [:=;] # Eyes
            [oO\-]? # Nose (optional)
            [D\)\]\(\]/\\OpP] # Mouth
        )"""

    RT_mentions_str = r'(?:RT @[\w_]+[:])'
    url_str = r'http[s]?://(?:[a-z]|[0-9]|[$-_@.&amp;+]|[!*\(\),]|(?:%[0-9a-f][0-9a-f]))+'
    html_str = r'<[^>]+>'
    hashtag_str = r"(?:\#+[\w_]+[\w\'_\-]*[\w_]+)"
    cashtag_str = r"(?:\$+[\w_]+[\w\'_\-]*[\w_]+"

    regex_remove = [url_str, html_str, emoticons_str, hashtag_str, RT_mentions_str]

    regex_str = [
        r'(?:(?:\d+,?)+(?:\.?\d+)?)',  # numbers
        r"(?:[a-z][a-z'\-_]+[a-z])",  # words with - and '
        r'(?:[\w_]+)',  # other words
        r'(?:\S)'  # anything else
    ]

    tokens_re = re.compile(r'(' + '|'.join(regex_str) + ')', re.VERBOSE | re.IGNORECASE)
    emoticon_re = re.compile(r'^' + emoticons_str + '$', re.VERBOSE | re.IGNORECASE)

    def remove(content):
        return re.sub(r'(' + '|'.join(regex_remove) + ')', '', content, re.VERBOSE | re.IGNORECASE)

    def tokenize(text):
        return tokens_re.findall(text)



    def preprocess(content, lowercase=False):
        text = remove(content)
        #tokens = tokenize(text)
        #if lowercase:
        #    tokens = [token if emoticon_re.search(token) else token.lower() for token in tokens]
        return text

    return preprocess(content)

def analyze_tweets():
    #for company in RSSFeeds._by_company:
        #Reading Tweets

        tweets_data_path ='C:\\Users\\Open Account\\Documents\\20180114_1100twitter_streaming.json'

        company_symbols = []

        for company in RSSFeeds._by_company:
            company_symbol = company.company_Feed.replace("$", "")
            company_symbols.append(company_symbol)

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

        nosymbol_count = 0
        noreference_count = 0

        print("Processing Tweets ...")

        for tweet in tweets_data:
            #Dictionary to store information about Tweet
            parsed_tweet = {}

            parsed_tweet['id'] = tweet['id_str']

            #Get text and language
            parsed_tweet['text'] = tweet['text']
            parsed_tweet['text_clean'] = clean_text(tweet['text'])
            print(parsed_tweet['text_clean'])
            parsed_tweet['lang'] = tweet['lang']
            ts = tweet['created_at']

            #Get Timestamp and adjust Timestamp to EST
            fmt = '%a %b %d %H:%M:%S %z %Y'
            timestamp = datetime.datetime.strptime(ts, fmt)

            # adjusting timestamp to EST
            EST = timezone('EST')
            fmt_adj = '%Y-%m-%d %H:%M:%S'
            adjusting = timestamp.astimezone(EST).strftime(fmt_adj)
            timestamp_adj = datetime.datetime.strptime(adjusting, fmt_adj)
            parsed_tweet['time_adj'] = str(timestamp_adj.time())
            parsed_tweet['time_fetched'] = str(timestamp.time())
            parsed_tweet['date'] = timestamp_adj.date()

            # converting timestamp to integer to classify tweets by "timeslot"
            def to_integer(ts):
                return 100 * ts.hour + ts.minute
            time_int = to_integer(timestamp_adj.time())

            if time_int > 930 and time_int < 1600:
                parsed_tweet["timeslot"] = "during"
            else:
                if time_int > 0 and time_int < 930:
                    parsed_tweet["timeslot"] = "before"
                if time_int > 1600 and time_int < 2359:
                    parsed_tweet["timeslot"] = "after"

            #Get information so evaluate popularity of tweet
            parsed_tweet['retweets'] = tweet['retweet_count']
            parsed_tweet['favorite'] = tweet['favorite_count']
            user_dict = tweet['user']
            parsed_tweet['user_id'] = user_dict['id_str']
            parsed_tweet['user_followers'] = user_dict['followers_count']

            #Get Symbols to reference tweet to company
            entities = tweet['entities']
            tweet_symbols_dict = entities['symbols']
            tweet_symbols = []

            for tweet_symbol_dict in tweet_symbols_dict:
                tweet_symbols.append(tweet_symbol_dict['text'].upper())

            if tweet_symbols:
                parsed_tweet['symbols'] = tweet_symbols

            referenced_company = [x for x in company_symbols if x in tweet_symbols]

            if referenced_company:
               #parsed_tweet['reference'] = referenced_company
                parsed_tweet['reference'] = ''.join(referenced_company)

            for company_symbol in company_symbols:
                parsed_tweet['xref_{}'.format(company_symbol)] = company_symbol in parsed_tweet['reference']

            #analyzing relevance
            parsed_tweet['relevant'] = word_in_text(tweet['text'])

            RT_mentions_str = r'(?:RT @[\w_]+[:])'
            if re.search(RT_mentions_str, tweet['text'], re.VERBOSE | re.IGNORECASE):
                parsed_tweet['retweet'] = 'RT'

            analyzed_tweets.append(parsed_tweet)

        #create dataframe
        df_tweets = pd.DataFrame(analyzed_tweets)
        df_tweets = df_tweets.drop_duplicates(subset='id')
        df_tweets = df_tweets.set_index('date')
        #df_tweets.to_excel('C:\\Users\\Open Account\\Documents\\BA_JonasIls\\Twitter_Streaming\\alltweets.xls', columns=['text'])

        #analyzing the stream
        nosymbol_count = df_tweets['symbols'].isnull().sum()
        noreference_count = df_tweets['reference'].isnull().sum()

        nosymbol_ratio = nosymbol_count / len(df_tweets.index)
        noreference_ratio = noreference_count / len(df_tweets.index)

        #identifying unreferenced tweets and writing them to excel
        #rows = df_tweets.loc[df_tweets['reference'].isnull()]
        #rows.to_excel('C:\\Users\\Open Account\\Documents\\BA_JonasIls\\Twitter_Streaming\\noreference.xls')

        for company_symbol in company_symbols:

            rows = df_tweets.loc[df_tweets['xref_{}'.format(company_symbol)] == 'True']
            rows.to_csv('C:\\Users\\Open Account\\Documents\\BA_JonasIls\\Twitter_Streaming\\Twitterfeed_{}.csv'.format('company_symbol'),
                                index_label='date',
                                encoding="utf-8")

        print(len(df_tweets.index))
        print('No Reference')
        print(noreference_count)
        print(noreference_ratio)
        print('No Symbol')
        print(nosymbol_count)
        print(nosymbol_ratio)

        print(df_tweets['time_adj'].head(1))
        print(df_tweets['time_adj'].tail(1))

        print(df_tweets)

        return(df_tweets)

if __name__=='__main__':
    analyze_tweets()

    #df_old = pd.read_csv('C:\\Users\\Open Account\\Documents\\BA_JonasIls\\Twitter_Streaming\\Newsfeed_{}.csv'.format('MSFT'),
    #            encoding="utf-8",
    #            index_col='date')
    #print(df_old)