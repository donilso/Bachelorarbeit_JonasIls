import feedparser
import datetime
from pytz import timezone
import pandas as pd
from newspaper import Article
#____________________________________________________

''' Creating Class of RSS Feed '''
#____________________________________________________

# Creating Meta Class for Iteration
import feedparser
import datetime
from pytz import timezone
import pandas as pd
from newspaper import Article
#____________________________________________________

''' Creating Class of RSS Feed '''
#____________________________________________________

import feedparser
import datetime
from pytz import timezone
from newspaper import Article

class RSS_Iter(type):
    def __iter__(self):
        return self.classiter()

class RSSFeeds:
    __metaclass__ = RSS_Iter
    _by_company = [] # Store the stuff here...

    def __init__(self, company_Feed, url_Feed):
        self.url_Feed = url_Feed
        self.company_Feed = company_Feed
        self._by_company.append(self)

    @classmethod
    def classiter(cls): # iterate over class by giving all instances which have been instantiated
        return iter(cls.by_company.values())


AMZN = RSSFeeds(company_Feed = "AMZN", url_Feed='http://articlefeeds.nasdaq.com/nasdaq/symbols?symbol=AMZN')
MSFT = RSSFeeds(company_Feed = "MSFT"; url_Feed='http://articlefeeds.nasdaq.com/nasdaq/symbols?symbol=MSFT')


for RSSFeed in RSSFeeds:

    feed = []

    fetched_feed = feedparser.parse(RSSFeed)

    for entry in fetched_feed.entries:
        parsed_link = {}

        parsed_link['link'] = entry.link

        '''Create Timestamp adjusted to EST'''
        #published = entry.published

        # formats to convert timestamps
        #fmt = '%a, %d %b %Y %H:%M:%S %z'
        #timestamp = datetime.datetime.strptime(published, fmt)

        # adjusting timestamp to Eastern Standard Time
        #EST = timezone('EST')
        #fmt_adj = '%Y-%m-%d %H:%M:%S'

        #adjusting_tz = timestamp.astimezone(EST).strftime(fmt_adj)
        #parsed_link['datetime'] = [datetime.datetime.strptime(adjusting_tz, fmt_adj)]

        # extract atricle from HTML with newspaper lib
        article = Article(parsed_link['link'])
        article.download()
        article.parse()
        content = article.text
        parsed_link['article'] = content

        #response = get(parsed_link['link'])
        #extractor = Goose()
        #article = extractor.extract(raw_html=response.content)
        #parsed_link['article'] = article.cleaned_text

        feed.append(parsed_link)

    print(feed)
