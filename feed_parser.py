import feedparser
import datetime
from pytz import timezone
import pandas as pd
from newspaper import Article
import csv
import unidecode

### CONTENT ###
### 1. Creating CLASS of RSS Feed
### 2. Declaring the INSTANCES of our class
### 3. Main Function to fetch feeds and scrape news

#________________________________________________________
#________________________________________________________
''' 1. Creating CLASS of RSS Feed '''

class RSSFeeds(object):

    # Creating List of instances to make class iterable
    _by_company = []

    # Declaring attributes of instances
    def __init__(self, url_Feed, company_Feed):
        self.url_Feed = url_Feed
        self._by_company.append(self)

        self.company_Feed = company_Feed

#________________________________________________________
#________________________________________________________
''' 2. Declaring the INSTANCES of our class'''

MSFT = RSSFeeds(url_Feed=['https://finance.google.com/finance/company_news?q=NASDAQ:MSFT&ei=Ya0tWomUMIugswGAqLrwDg&output=rss',
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

#________________________________________________________
#________________________________________________________
'''3. Main Function to fetch feeds and scrape news'''

if __name__ == "__main__":
    for company in RSSFeeds._by_company:

        # List to Store data
        feed = []

        # List of Links to sort out duplicates
        # links =[]

        # fetching every feed per company
        for URL in company.url_Feed:

            fetched_feed = feedparser.parse(URL)


            # parsing every entry of feed
            for entry in fetched_feed.entries:

                # dictionary to store data
                parsed_link = {}

                parsed_link['link'] = unidecode.unidecode(entry.link)

                # parse timestamp
                p = entry.published.replace("Z", "UTC")
                published = p.replace(",","")

                ts_weekday , ts_day, ts_month, ts_year, ts_time, ts_timezone = published.split(" ")

                # identifying the format of timezone
                if '+' in ts_timezone:
                    fmt = '%a %d %b %Y %H:%M:%S %z'
                elif '-' in ts_timezone:
                    fmt = '%a %d %b %Y %H:%M:%S %z'
                else:
                    fmt = '%a %d %b %Y %H:%M:%S %Z'

                timestamp_fetched = datetime.datetime.strptime(published, fmt)

                # adjusting timestamp to Eastern Standard Time
                EST = timezone('EST')
                fmt_adj = '%Y-%m-%d %H:%M:%S'
                adjusting_tz = timestamp_fetched.astimezone(EST).strftime(fmt_adj)

                #timestamp_fetched = timestamp.strftime(fmt_adj)
                timestamp_adj = datetime.datetime.strptime(adjusting_tz, fmt_adj)

                parsed_link['time_fetched'] = timestamp_fetched.time()
                parsed_link['time_adj'] = timestamp_adj.time()
                parsed_link['date'] =timestamp_adj.date()

                # classifying news
                def to_integer(ts):
                    return 100 * ts.hour + ts.minute

                time_int = to_integer(timestamp_adj.time())

                if time_int > 930 and time_int < 1600:
                    parsed_link["Timeslot"] = "DURING"


                else:
                    if time_int > 0 and time_int < 930:
                        parsed_link["Timeslot"] = "BEFORE"

                    if time_int > 1600 and time_int < 2359:
                        parsed_link["Timeslot"] = "AFTER"

                # extract atricle from HTML with newspaper lib
                try:
                    article = Article(parsed_link['link'])
                    article.download()
                    article.parse()
                    content = article.text
                    parsed_link['article'] = unidecode.unidecode(content)

                except:
                    parsed_link['article'] = "Error accessing {}".format(parsed_link['link'])

                # append link to list of links to sort out duplicate links in the next step
                # links.append(parsed_link['link'])

                # append data of every article to list
                if parsed_link not in feed:
                    feed.append(parsed_link)

        df_new = pd.DataFrame(feed).set_index('date')
        print(df_new)
        df_old = pd.read_csv(
            'C:\\Users\\Open Account\\Documents\\BA_JonasIls\\Newsfeed_{}.csv'.format(company.company_Feed),
            encoding="utf-8", index_col='date')


        df_merged = pd.concat([df_old, df_new])
        df_merged = df_merged.drop_duplicates(subset='link')

        df_merged.to_csv(
            'C:\\Users\\Open Account\\Documents\\BA_JonasIls\\Newsfeed_{}.csv'.format(company.company_Feed),
            index_label='date', encoding="utf-8")

        #df_new.to_csv('C:\\Users\\Open Account\\Documents\\BA_JonasIls\\Newsfeed_{}.csv'.format(company.company_Feed),
        #    index_label='date', encoding="utf-8")

        #print("DF Merged")
        print(len(df_merged))