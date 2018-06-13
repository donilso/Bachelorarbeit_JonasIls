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

def write_data(df, file_path_write, company):
    with open(file_path_write.format(company), 'a') as f:
        df.to_csv(f, sep='#', encoding='utf-8', index_label='date', header=False,
                  columns=['date', 'id',  'text_clean', 'time_adj', 'user_followers', 'retweet', 'timeslot', 'reference', 'symbols'])


def write_data_one(df, file_path_write, company):
    with open(file_path_write.format(company), 'a') as f:
        df.to_csv(f, sep='#', encoding='utf-8', index_label='date', header=True,
                  columns=['date', 'id', 'text_clean', 'time_adj', 'user_followers', 'retweet', 'timeslot', 'reference','symbols'])


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
    mentions_str = r'(?:@[\w_])'
    url_str = r'http[s]?://(?:[a-z]|[0-9]|[$-_@.&amp;+]|[!*\(\),]|(?:%[0-9a-f][0-9a-f]))+'
    html_str = r'<[^>]+>'
    hashtag_str = r"(?:\#+[\w_]+[\w\'_\-]*[\w_]+)"
    cashtag_str = r"(?:\$+[\w_]+[\w\'_\-]*[\w_]+)"
    non_ascii_str = r'[^\x00-\x7f]'

    regex_remove = [url_str, html_str, emoticons_str, RT_mentions_str, non_ascii_str, mentions_str]

    regex_str = [
        r'(?:(?:\d+,?)+(?:\.?\d+)?)',  # numbers
        r"(?:[a-z][a-z'\-_]+[a-z])",  # words with - and '
        r'(?:[\w_]+)',  # other words
        r'(?:\S)'  # anything else
    ]

    #retweet_re = re.compile(r'(' + '|'.join(RT_mentions_str) + ')', re.VERBOSE | re.IGNORECASE)
    tokens_re = re.compile(r'(' + '|'.join(regex_str) + ')', re.VERBOSE | re.IGNORECASE)
    emoticon_re = re.compile(r'^' + emoticons_str + '$', re.VERBOSE | re.IGNORECASE)

    def text_from_bits(bits, encoding='utf-8', errors='surrogatepass'):
        n = int(bits, 2)
        return n.to_bytes((n.bit_length() + 7) // 8, 'big').decode(encoding, errors) or '\0'

    def remove(content):
        content = content.replace('#', ' ')
        content = content.replace('\r', '')
        content = content.replace('\n', '')
        content = re.sub (cashtag_str, 'stock', content, re.VERBOSE | re.IGNORECASE)
        content =  re.sub(r'(' + '|'.join(regex_remove) + ')', '', content, re.VERBOSE | re.IGNORECASE)
        return(content)

    def tokenize(text):
        return tokens_re.findall(text)

    def preprocess(content, lowercase=False):
        text = remove(content)
        #tokens = tokenize(text)
        #if lowercase:
        #    tokens = [token if emoticon_re.search(token) else token.lower() for token in tokens]
        return text

    return preprocess(content)


def read_bigfile(file_path, file_path_write, function_call):

    companies = ['$MSFT', '$MMM', '$AXP', '$AAPL', '$BA', '$CAT', '$CVX', '$CSCO', '$KO', '$DWDP', '$DIS', '$XOM',
                 '$GE', '$GS', '$HD', '$IBM', '$INTC', '$JNJ', '$JPM', '$MCD', '$MRK', '$NKE', '$PFE', '$PG', '$TRV',
                 '$UTX', '$UNH', '$VZ', '$V', '$WMT']
    companies = [company.replace('$', '') for company in companies]

    print('Open File...')
    with open(file_path, encoding="utf-8") as tweets_file:
        counter = 0
        write_count = 0
        badtweets_counter = 0
        tweets_data = list()

        for line in tweets_file:

            try:
                tweet = json.loads(line)
                counter = counter + 1
                tweets_data.append(tweet)

            except Exception as e:
                print(e)

            if len(tweets_data) == 10000:
                print("Tweet No. {}".format(counter))
                analyzed_tweets = list()

                for tweet in tweets_data:

                    try:
                        parsed_tweet = {}

                        parsed_tweet['id'] = tweet['id_str']

                        parsed_tweet['text_clean'] = clean_text(tweet['text'])
                        ts = tweet['created_at']

                        # Get Timestamp and adjust Timestamp to EST
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
                            return 10000 * ts.hour + 100 * ts.minute + ts.second

                        time_int = to_integer(timestamp_adj.time())

                        if time_int > 93000 and time_int < 160000:
                            parsed_tweet["timeslot"] = "during"
                        else:
                            if time_int > 0 and time_int < 93000:
                                parsed_tweet["timeslot"] = "before"
                            if time_int > 160000 and time_int < 235959:
                                parsed_tweet["timeslot"] = "after"

                        # Get follower count of user
                        user_dict = tweet['user']
                        parsed_tweet['user_followers'] = user_dict['followers_count']

                        # Get Symbols to reference tweet to company
                        entities = tweet['entities']
                        tweet_symbols_dict = entities['symbols']
                        tweet_symbols = []

                        for tweet_symbol_dict in tweet_symbols_dict:
                            tweet_symbols.append(tweet_symbol_dict['text'].upper())

                        if tweet_symbols:
                            parsed_tweet['symbols'] = tweet_symbols

                        referenced_company = [x for x in companies if x in tweet_symbols]

                        if len(tweet_symbols) == 1:
                            parsed_tweet['reference'] = referenced_company

                            for company in companies:
                                if company in referenced_company:
                                    a = "True"
                                else:
                                    a = "False"

                                parsed_tweet['xref_{}'.format(company)] = a

                        # identify retweets
                        RT_mentions_str = r'(?:RT @[\w_]+[:])'
                        retweet_re = re.compile(RT_mentions_str, re.IGNORECASE)
                        matches = retweet_re.search(tweet['text'])
                        if matches:
                            parsed_tweet['retweet'] = '1'
                        else:
                            parsed_tweet['retweet'] = '0'

                        analyzed_tweets.append(parsed_tweet)

                    except Exception as e:
                        badtweets_counter = badtweets_counter + 1
                        print(tweet)
                        print(e)

                        with open('C:\\Users\\jonas\\Documents\\BA_JonasIls\\Twitter_Streaming\\Feeds\\20180613_BadTweets', 'a', encoding="utf-8") as f:
                            f.write('{} for : {} \r\n'.format(e, tweet))

                        continue

                write_count = write_count + 1
                # create dataframe
                df_tweets = pd.DataFrame(analyzed_tweets)
                df_tweets = df_tweets.drop_duplicates(subset='id')
                df_tweets = df_tweets.set_index('date')

                # write dataframe for anycompany to csv
                for company in companies:
                    rows_ref = df_tweets.loc[df_tweets['xref_{}'.format(company)] == 'True']

                    if write_count == 1 and function_call == 1:
                        write_data_one(rows_ref, file_path_write, company)
                    else:
                        write_data(rows_ref, file_path_write, company)

                analyzed_tweets.clear()
                tweets_data.clear()

                continue

            else:
                continue


        analyzed_tweets = list()

        for tweet in tweets_data:

            try:
                parsed_tweet = {}

                parsed_tweet['id'] = tweet['id_str']

                parsed_tweet['text_clean'] = clean_text(tweet['text'])
                ts = tweet['created_at']

                # Get Timestamp and adjust Timestamp to EST
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
                    return 10000 * ts.hour + 100 * ts.minute + ts.second

                time_int = to_integer(timestamp_adj.time())

                if time_int > 93000 and time_int < 160000:
                    parsed_tweet["timeslot"] = "during"
                else:
                    if time_int > 0 and time_int < 93000:
                        parsed_tweet["timeslot"] = "before"
                    if time_int > 160000 and time_int < 235959:
                        parsed_tweet["timeslot"] = "after"

                # Get follower count of user
                user_dict = tweet['user']
                parsed_tweet['user_followers'] = user_dict['followers_count']

                # Get Symbols to reference tweet to company
                entities = tweet['entities']
                tweet_symbols_dict = entities['symbols']
                tweet_symbols = []

                for tweet_symbol_dict in tweet_symbols_dict:
                    tweet_symbols.append(tweet_symbol_dict['text'].upper())

                if tweet_symbols:
                    parsed_tweet['symbols'] = tweet_symbols

                referenced_company = [x for x in companies if x in tweet_symbols]

                if len(tweet_symbols) == 1:
                    parsed_tweet['reference'] = referenced_company

                    for company in companies:
                        if company in referenced_company:
                            a = "True"
                        else:
                            a = "False"

                        parsed_tweet['xref_{}'.format(company)] = a

                # identify retweets
                RT_mentions_str = r'(?:RT @[\w_]+[:])'
                retweet_re = re.compile(RT_mentions_str, re.IGNORECASE)
                matches = retweet_re.search(tweet['text'])
                if matches:
                    parsed_tweet['retweet'] = '1'
                else:
                    parsed_tweet['retweet'] = '0'

                analyzed_tweets.append(parsed_tweet)

            except Exception as e:
                badtweets_counter = badtweets_counter + 1
                print(tweet)
                print(e)

                with open('C:\\Users\\jonas\\Documents\\BA_JonasIls\\Twitter_Streaming\\Feeds\\20180613_BadTweets', 'a', encoding="utf-8") as f:
                    f.write('{} for : {} \r\n'. format(e, tweet))

        df_tweets = pd.DataFrame(analyzed_tweets)
        df_tweets = df_tweets.drop_duplicates(subset='id')
        df_tweets = df_tweets.set_index('date')

        # write dataframe for anycompany to csv
        for company in companies:
            rows_ref = df_tweets.loc[df_tweets['xref_{}'.format(company)] == 'True']
            write_data(rows_ref, file_path_write, company)

        print("BAD TWEETS COUNT: {}".format(badtweets_counter))


if __name__=='__main__':
    #files = ['01_20180101twitter_streaming.json', '02_20180107twitter_streaming.json', '03_20180108twitter_streaming.json', '04_20180112twitter_streaming01.json','05_20180112twitter_streaming02.json', '06_20180112twitter_streaming03.json']

    files = ['07_20180305_twitter_streaming.json', '08_20160401twitter_streaming.json']
    function_call = 0
    for file in files:
        file_path_read = 'C:\\Users\\jonas\\Documents\\BA_JonasIls\\Twitter_Streaming\\Feeds\\Rohdaten\\{}'.format(file)
        file_path_write = 'C:\\Users\\jonas\\Documents\\BA_JonasIls\\Twitter_Streaming\\Feeds\\20180613\\' \
                          '20180305_20180613_twitterstreaming_{}.csv'
        function_call = function_call + 1
        read_bigfile(file_path_read, file_path_write, function_call)

