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
'''3. Main Function to fetch feeds and scrape news'''

def main():

    # iterating over all companies in class
    for RSSFeed in RSSFeeds._by_company:
        # List to Store data
        feed = []

        # List of Links to sort out duplicates
        # links =[]

        # fetching every feed per company
        for URL in RSSFeed.url_Feed:

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
                parsed_link['datetime'] = str(timestamp_adj)

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
                article = Article(parsed_link['link'])
                article.download()
                article.parse()
                content = article.text
                parsed_link['article'] = unidecode.unidecode(content)

                # append link to list of links to sort out duplicate links in the next step
                # links.append(parsed_link['link'])

                # append data of every article to list
                if parsed_link not in feed:
                    feed.append(parsed_link)

        print(len(feed))

        # safe dictionaries to csv dropping the keys and writing them as header
        keys = feed[0].keys()
        with open('C:\\Users\\Open Account\\Documents\\BA_Jonas\\Newsfeed_{}.csv'.format(RSSFeed.company_Feed), 'a') as f:
            dict_writer = csv.DictWriter(f, keys)
            dict_writer.writeheader()
            dict_writer.writerows(feed)

        print(feed)
        print(len(feed))


main()