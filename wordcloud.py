import pandas as pd
from wordcloud import WordCloud
from io import StringIO
import re
import matplotlib.pyplot as plt


class company_names(object):

    # Creating List of instances to make class iterable
    _by_company = []

    # Declaring attributes of instances
    def __init__(self, identifier):
        self._by_company.append(identifier)
        self.identifier = identifier

AAPL = company_names(identifier = ['apple', 'aapl', 'appleinc'])
MSFT = company_names (identifier =['microsoft', 'msft'])
MMM = company_names(identifier = ['3m', 'mmm'] )
AXP = company_names(identifier = ['american express', 'amex', 'axp'] )
BA = company_names(identifier = ['boeing', ' ba '])
CAT = company_names(identifier = ['caterpillar', ' cat '])
CVX = company_names(identifier = ['chevron', 'cvx'])
CSCO = company_names(identifier = ['cisco', 'csco'])
KO = company_names(identifier = ['coca', 'cola', 'coca cola', 'cocacolacompany'])
DWDP = company_names(identifier = ['dow du pont', 'du pont', 'dwdp', 'dowdupont'])
DIS = company_names(identifier = ['walt', 'disney', 'waltdisney', ' dis '])
XOM = company_names(identifier = ['exxon', 'mobil', 'exxonmobil', ' xom '])
GE = company_names(identifier = ['general', 'electric', 'sgeneral', 'generalelectric', ' ge '])
GS = company_names(identifier = [' gs ', 'goldman', 'sachs', 'goldmansachs'])
HD = company_names(identifier = ['hd', 'home', 'depot', 'homedepot'])
IBM = company_names(identifier = ['international', 'buiness', 'machines', 'machine', 'mach', 'businessmachines', 'ibm'])
INTC = company_names(identifier = ['intel', 'ntel', 'intc'])
JNJ = company_names(identifier = ['johnson', 'johnsonandjohnson', 'jnj'])
JPM = company_names(identifier = ['jp', 'morgan', 'chase', 'jpmorgan'])
MCD = company_names(identifier = ['mcdonald', 'mcd', 'mc', 'donald'])
MRK = company_names(identifier = ['merck', 'mrk'])
NKE = company_names(identifier = ['nike', 'snike', 'nke'])
PFE = company_names(identifier = ['pfizer', 'spfizer', ' pfe '])
PG = company_names(identifier = ['procter', 'gamble', ' pg '])
TRV = company_names(identifier = ['travelers', 'companies', 'trv'])
UTX = company_names(identifier = ['utx', 'united', 'technologies', 'unitedtechnologies'])
UNH = company_names(identifier = ['united', 'health', 'unitedhealth', 'unh'])
VZ = company_names(identifier = ['vz', 'verizon', 'communications', 'verizoncommunications'])
V = company_names(identifier = ['visa'])
WMT = company_names(identifier = ['wal', 'mart', 'walmart', 'wmt'])



def black_color_func(word, font_size, position, orientation, random_state=None,
                    **kwargs):
    return "hsl(0, 0%, 0" \
           "%)"


def threshold_sentiment(df_sent, sent_dict, percentile):
    #print("DF SENT FOR THRESHOLD:", len(df_sent))

    # lists to store all neg / pos sentiment scores
    values_neg = []
    values_pos = []
    # counter to track the number of 0-Sentiments
    null_counter = 0

    # iterating over the tweets
    for index, tweet in df_sent.iterrows():
        sent = tweet[sent_dict]

        if sent < 0:
            values_neg.append(sent)
        elif sent > 0:
            values_pos.append(sent)


    # calculating the minimal sentiment based on the percentile given
    try:
        sent_min_pos = np.percentile(values_pos, percentile)
    except:
        sent_min_pos = 0

    try:
        sent_min_neg = np.percentile(values_neg, (100 - percentile))
    except:
        sent_min_neg = 0

    return df_sent.loc[(df_sent[sent_dict] >= sent_min_pos) | (df_sent[sent_dict] <= (sent_min_neg))]


def open_df_sent(company):
    file_path = 'C:\\Users\\jonas\\Documents\\BA_JonasIls\\Twitter_Streaming\\Sentiment_Dataframes\\20180101_20180410\\20180101_20180410_SentimentDataframes_{}'.format(company)
    df_tweets = pd.read_csv(file_path, encoding="utf-8")

    df_tweets['text_clean'] = df_tweets['text_clean'].astype(str)
    df_tweets['date'] = pd.to_datetime(df_tweets['date'], errors='coerce')

    return df_tweets


def open_df_news(company):
    file_path = 'C:\\Users\\jonas\\Documents\\BA_JonasIls\\Newsfeeds\\Sentiment_Dataframes\\NewsSentimentDataframes_{}.csv'.format(
        company)
    df_tweets = pd.read_csv(file_path, encoding="utf-8")
    df_tweets['article_clean'] = df_tweets['article_clean'].astype(str)
    df_tweets['date'] = pd.to_datetime(df_tweets['date'], errors='coerce')

    # adding sentiment calculated with textblob
    #df_tweets['SentimentTB'] = df_tweets['text_clean'].apply(get_TBSentiment)
    #df_tweets.text_clean.to_excel('C:\\Users\\jonas\\Documents\\BA_JonasIls\\text_clean_spamfree.xls', encoding='utf-8')
    return df_tweets


companies = ['$MSFT', '$AAPL', '$MMM', '$AXP', '$BA', '$CAT', '$CVX', '$CSCO', '$KO', '$DWDP', '$DIS', '$XOM', '$GE','$GS', '$HD', '$IBM', '$INTC', '$JNJ', '$JPM', '$MCD', '$MRK', '$NKE', '$PFE', '$PG', '$TRV', '$UTX', '$UNH', '$VZ', '$V', '$WMT']
companies = [company.replace('$', '') for company in companies]

all_text = ''


stopwords = ['stock', 'market ', 'million ', 'billion ', 'management ', 'asset ', 'capital ', 'stake ', 'share ', 'business ', 'system ', 'dow jones ', 'think ', 'fourth quarter ', 'last quarter ', 'q4 ', 'nasdaq ', 'nyse ',
             'company ', 'companies ','Corp ', 'corp ','amp ', 'llc ', 'inc ', 'ltd ', 'holding ', ' co ', 'co. ', 'group ', 'iph ', 'firm ',
             'week ', 'will ', 'store ', 'new ', 'ci ', 'year ', 'one ', 'now ' , 'see ', 'say ', 'day ', 'today ', 'said ', 'still ', 'month ', 'back ', 'another ', 'use ', 'state ', 'come ', 'according ', 'beca ',
             'need ', 'end ', 'industry ',
             'two ', 'three ', 'make ', 'even ']

x = [',', '.', '?', '!', ';', ':', '"', '(', ')']

for company in company_names._by_company:
    stopwords.extend(company)

print(stopwords)
stopwords.sort(key=len, reverse=True)
print(stopwords)

for i in x:
    all_text = all_text.replace(i, " ")

for stopword in stopwords:
    all_text = all_text.replace(stopword, " ")

quantiles = [0, 50, 75, 90]

for q in quantiles:
    # open df
    df = open_df_sent('AllStocks')
    print(len(df))

    # select 10%-Qantile of most sentimental tweets
    df = threshold_sentiment(df, 'SentimentHE', q)
    df_relevant = df.loc[df.SentimentHE > 0]

    # fill string with all text in relevant tweets
    all_text = ''
    for index, tweet in df_relevant.iterrows():
        text = tweet.article_clean.lower()
        all_text = all_text + text

    for company in company_names._by_company:
        stopwords.extend(company)

    print(stopwords)
    stopwords.sort(key=len, reverse=True)
    print(stopwords)

    for i in x:
        all_text = all_text.replace(i, " ")

    for stopword in stopwords:
        all_text = all_text.replace(stopword, " ")

    # lower max_font_size
    wordcloud = WordCloud(max_words=100, max_font_size=40,
                          background_color='white', color_func=black_color_func).generate(all_text)
    plt.figure()
    plt.imshow(wordcloud, interpolation="bilinear")
    plt.axis("off")
    #plt.show()
    plt.savefig('C:\\Users\\jonas\\Documents\\BA_JonasIls\\Literatur & Analysen\\Plots\\WordCloud\\Quantiles\\news_wordcloud_bigrams_pos_{}'.format(q))

