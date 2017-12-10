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

AMZN = RSSFeeds(url_Feed=['http://articlefeeds.nasdaq.com/nasdaq/symbols?symbol=AMZN','http://articlefeeds.nasdaq.com/nasdaq/symbols?symbol=AMZN'], company_Feed='AMZN')
MSFT = RSSFeeds(url_Feed=['http://articlefeeds.nasdaq.com/nasdaq/symbols?symbol=MSFT'], company_Feed='MSFT')


#________________________________________________________
#________________________________________________________
''' Open old dataframe '''
#def open_olddata():
    #for company in RSSFeeds._by_company:
        #df_old = pd.read_csv('C:\\Users\\Open Account\\Documents\\BA_Jonas\\Newsfeed_{}.csv'.format(company.company_Feed), encoding="utf-8")
        #return df_old

#________________________________________________________
#________________________________________________________
'''3. Main Function to fetch feeds and scrape news'''


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
            published = entry.published.replace("Z", "UTC")

            # formats to convert string to datetime
            fmt = '%a, %d %b %Y %H:%M:%S %Z'
            timestamp = datetime.datetime.strptime(published, fmt)

            # adjusting timestamp to Eastern Standard Time
            EST = timezone('EST')
            fmt_adj = '%Y-%m-%d %H:%M:%S'
            adjusting_tz = timestamp.astimezone(EST).strftime(fmt_adj)
            timestamp_adj = datetime.datetime.strptime(adjusting_tz, fmt_adj)
            parsed_link['date'] = unidecode.unidecode(str(timestamp_adj))

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

            extractor = Extractor(extractor='Article Extractor', url=parsed_link['link'])
            content = extractor.get_text()
            parsed_link['article'] = unidecode.unidecode(content)

            # append link to list of links to sort out duplicate links in the next step
            # links.append(parsed_link['link'])

            # append data of every article to list
            if parsed_link not in feed:
                feed.append(parsed_link)

    df_new = pd.DataFrame(feed).set_index('date')

    print(df_new)


    #df_old = pd.read_csv(
    #    'C:\\Users\\Open Account\\Documents\\BA_Jonas\\Newsfeed_{}.csv'.format(company.company_Feed),
    #    encoding="utf-8", index_col='date')

    #df_merged = pd.concat([df_old, df_new])
    #df_merged = df_merged.drop_duplicates(subset='link')

    #df_merged.to_csv(
    #    'C:\\Users\\Open Account\\Documents\\BA_Jonas\\Newsfeed_{}.csv'.format(company.company_Feed),
    #    index_label='date', encoding="utf-8")

    #df_new.to_csv(
    #    'C:\\Users\\Open Account\\Documents\\BA_Jonas\\Newsfeed_{}.csv'.format(company.company_Feed),
    #    index_label='date', encoding="utf-8")

