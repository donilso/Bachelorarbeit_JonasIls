import feedparser
import datetime
from pytz import timezone
import pandas as pd
from newspaper import Article
#____________________________________________________

''' Creating Class of RSS Feed '''
#____________________________________________________

# Creating Meta Class for Iteration
class it(type):
    def __iter__(self):
        return self.classiter()

class RSSFeeds:
    __metaclass__ = it
    by_company = {} # Store the stuff here...

    def __init__(self, url_Feed):
        self.url_Feed = url_Feed
        self.by_company[url_Feed] = self

    @classmethod
    def classiter(cls): # iterate over class by giving all instances which have been instantiated

        feed = []

        fetched_feed = feedparser.parse(cls.url_Feed)

        for entry in fetched_feed.entries:
            parsed_link = {}

            parsed_link['link'] = entry.link

            '''Create Timestamp adjusted to EST'''
            published = entry.published

            # formats to convert timestamps
            fmt = '%a, %d %b %Y %H:%M:%S %z'
            timestamp = datetime.datetime.strptime(published, fmt)

            # adjusting timestamp to Eastern Standard Time
            EST = timezone('EST')
            fmt_adj = '%Y-%m-%d %H:%M:%S'

            adjusting_tz = timestamp.astimezone(EST).strftime(fmt_adj)
            parsed_link['datetime'] = [datetime.datetime.strptime(adjusting_tz, fmt_adj)]

            # extract atricle from HTML with newspaper lib
            content = Article(parsed_link['link'])
            parsed_link['article'] = content.text

            feed.append(parsed_link)

            print(feed)

        return iter(cls.by_company.values())


if __name__ == '__main__':
    AMZN = RSSFeeds(url_Feed='http://articlefeeds.nasdaq.com/nasdaq/symbols?symbol=AMZN')
    MSFT = RSSFeeds(url_Feed='http://articlefeeds.nasdaq.com/nasdaq/symbols?symbol=MSFT')