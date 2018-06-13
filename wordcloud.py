import pandas as pd
from wordcloud import WordCloud
from io import StringIO
import re
import matplotlib.pyplot as plt
from textblob import TextBlob


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


def get_TBSentiment(text):
    '''Function to calculate a sentiment score based on the textblob library'''
    analysis = TextBlob(text)
    SentimentTB = analysis.sentiment.polarity
    return(SentimentTB)


def open_df_sent(company):
    file_path = 'C:\\Users\\Open Account\\Documents\\BA_JonasIls\\Twitter_Streaming\\Sentiment_Dataframes\\20180101_20180217\\20180101_20180217_SentimentDataframes_{}'.format(company)
    df_tweets = pd.read_csv(file_path, encoding="utf-8")
    df_tweets = df_tweets

    df_tweets['text_clean'] = df_tweets['text_clean'].astype(str)
    df_tweets['date'] = pd.to_datetime(df_tweets['date'], errors='coerce')
    #df_tweets['user_followers'] = df_tweets['user_followers'].astype(int)

    # adding sentiment calculated with textblob
    df_tweets['SentimentTB'] = df_tweets['text_clean'].apply(get_TBSentiment)
    #df_tweets.text_clean.to_excel('C:\\Users\\Open Account\\Documents\\BA_JonasIls\\text_clean_spamfree.xls', encoding='utf-8')
    return df_tweets


def open_df_news(company):
    file_path = 'C:\\Users\\Open Account\\Documents\\BA_JonasIls\\Newsfeeds\\Sentiment_Dataframes\\NewsSentimentDataframes_{}.csv'.format(
        company)
    df_tweets = pd.read_csv(file_path, encoding="utf-8")
    df_tweets['article_clean'] = df_tweets['article_clean'].astype(str)
    df_tweets['date'] = pd.to_datetime(df_tweets['date'], errors='coerce')

    # adding sentiment calculated with textblob
    #df_tweets['SentimentTB'] = df_tweets['text_clean'].apply(get_TBSentiment)
    #df_tweets.text_clean.to_excel('C:\\Users\\Open Account\\Documents\\BA_JonasIls\\text_clean_spamfree.xls', encoding='utf-8')
    return df_tweets


companies = ['$MSFT', '$AAPL', '$MMM', '$AXP', '$BA', '$CAT', '$CVX', '$CSCO', '$KO', '$DWDP', '$DIS', '$XOM', '$GE','$GS', '$HD', '$IBM', '$INTC', '$JNJ', '$JPM', '$MCD', '$MRK', '$NKE', '$PFE', '$PG', '$TRV', '$UTX', '$UNH', '$VZ', '$V', '$WMT']
companies = [company.replace('$', '') for company in companies]

all_text = ''

#f = open('C:\\Users\\Open Account\\Documents\\BA_JonasIls\\newsclean_neg_allstocks.txt', 'r')
#all_text = f.read()
#f.close()

counter = 0
for company in companies:
    print(company)
    df = open_df_sent(company)
    df = df.loc[df.SentimentLM > 0]
    for index, tweet in df.iterrows():
        text = tweet.text_clean.lower()
        all_text = all_text + text
        counter = counter + 1
print(counter)

#with open('C:\\Users\\Open Account\\Documents\\BA_JonasIls\\newsclean_pos_allstocks.txt', 'w') as f:
#    f.write(all_text)
#    f.close()

stopwords = ['stock ', 'market ', 'million ', 'billion ', 'management ', 'asset ', 'capital ', 'stake ', 'share ', 'business ', 'system ', 'dow jones ', 'think ', 'fourth quarter ', 'last quarter ', 'q4 ', 'nasdaq ', 'nyse ',
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


# Generate a word cloud image
#wordcloud = WordCloud().generate(all_text)

# Display the generated image:
# the matplotlib way:
#plt.imshow(wordcloud, interpolation='bilinear')
#plt.axis("off")

# lower max_font_size
wordcloud = WordCloud(max_words=100, max_font_size=40, background_color='white', color_func=black_color_func).generate(all_text)
plt.figure()
plt.imshow(wordcloud, interpolation="bilinear")
plt.axis("off")
plt.show()