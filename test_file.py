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



GE = RSSFeeds(url_Feed = ['https://finance.google.com/finance/company_news?q=NYSE:GE&ei=YqstWoDnMZDDsAGv8pCACg&output=rss',
                          'http://finance.yahoo.com/rss/headline?s=GE'],
              company_Feed='$GE')

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

            if '+' in ts_timezone:
                fmt = '%a %d %b %Y %H:%M:%S %z'
            elif '-' in ts_timezone:
                fmt = '%a %d %b %Y %H:%M:%S %z'

            else:
                fmt = '%a %d %b %Y %H:%M:%S %Z'

            timestamp = datetime.datetime.strptime(published, fmt)

            # adjusting timestamp to Eastern Standard Time
            EST = timezone('EST')
            fmt_adj = '%Y-%m-%d %H:%M:%S'
            adjusting_tz = timestamp.astimezone(EST).strftime(fmt_adj)

            timestamp_fetched = timestamp.strftime(fmt_adj)
            timestamp_adj = datetime.datetime.strptime(adjusting_tz, fmt_adj)

            parsed_link['datetime_fetched'] = unidecode.unidecode(str(timestamp_fetched))
            parsed_link['date'] = unidecode.unidecode(str(timestamp_adj))

            print(parsed_link['datetime_fetched'], parsed_link['date'])

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
                parsed_link['atricle'] = "Error accessing {}".format(parsed_link['link'])

            # append link to list of links to sort out duplicate links in the next step
            # links.append(parsed_link['link'])

            # append data of every article to list
            if parsed_link not in feed:
                feed.append(parsed_link)

    df_new = pd.DataFrame(feed).set_index('date')

    print(df_new)