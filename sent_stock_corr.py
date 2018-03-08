import pandas as pd
import numpy as np
import pandas_datareader.data as web
from datetime import datetime, timedelta
from textblob import TextBlob
import statsmodels.api as sm
import math

def get_TBSentiment(text):
    '''Function to calculate a sentiment score based on the textblob library'''
    analysis = TextBlob(text)
    SentimentTB = analysis.sentiment.polarity
    return(SentimentTB)


def open_df_sent(company):
    file_path = 'C:\\Users\\Open Account\\Documents\\BA_JonasIls\\Twitter_Streaming\\Sentiment_Dataframes\\20180101_20180217\\20180101_20180217_SentimentDataframes_{}'.format(company)
    df_tweets = pd.read_csv(file_path, encoding="utf-8")
    df_tweets = df_tweets

    df_tweets = df_tweets.fillna(0)
    df_tweets['text_clean'] = df_tweets['text_clean'].astype(str)
    df_tweets['date'] = pd.to_datetime(df_tweets['date'])
    df_tweets['user_followers'] = df_tweets['user_followers'].astype(int)

    # adding sentiment calculated with textblob
    #df_tweets['SentimentTB'] = df_tweets['text_clean'].apply(get_TBSentiment)

    return df_tweets

def date_start(df_sent):
    return(df_sent.date.iloc[0])


def date_end(df_sent):
    return(df_sent.date.iloc[len(df_sent)-1])


def get_df_index(index):
    df_index = pd.read_csv('C:\\Users\\Open Account\\Documents\\BA_JonasIls\\Stock_Quotes\\20180201StockPrices_^{}.csv'.format(index), encoding='utf-8')
    df_index['daily_returns_index'] = df_index['Adj Close'] / df_index['Adj Close'].shift(1) - 1
    return df_index


def daily_yield(company, start, end):
    '''Function to parse daily stock quotes and calculate daily yields'''

    # parsing stockdata
    df_stock = pd.read_csv('C:\\Users\\Open Account\\Documents\\BA_JonasIls\\Stock_Quotes\\20180201StockPrices_{}.csv'.format(company))


    # calculating yields
    df_stock['daily_returns'] = df_stock['Adj Close']/df_stock['Adj Close'].shift(1)-1
    df_index = get_df_index('DJI')

    # contact dataframes
    df = pd.concat([df_stock, df_index['daily_returns_index']], axis=1)

    # calculating abnormal returns
    #df = df.drop(df.index[0])

    #model = pd.stats.ols.MovingOLS(y=df.daily_returns, x=df.daily_returns_index, window_type='rolling', window=100, intercept=True)
    #def model(endog):
    #    model = sm.OLS(endog=endog, exog=df.daily_returns_index, window=100).fit()
    #    return(model.params)
    #df['beta'] = pd.rolling_apply(df.daily_returns, window=100, func=model(df['daily_returns']))

    df.rolling_cov100 = pd.rolling_cov(arg1=df.daily_returns, arg2=df.daily_returns_index, window=100, min_periods=100)
    df.rolling_varIndex100 = pd.rolling_var(df.daily_returns_index, window=100)
    df['beta'] = df.rolling_cov100 / df.rolling_varIndex100
    df['abnormal_returns'] = df['daily_returns'] - df['daily_returns_index'] * df['beta']
    #df['abnormal_returns'] = df['daily_returns'] - df['daily_returns_index']
    #calculating parks volatility
    df['volatility_parks'] = ((np.log(df['High']-np.log(df['Low'])))**2) / (4 * np.log(2))

    # extract relevant time period
    df['Date'] = pd.to_datetime(df['Date'])
    one_day = timedelta(days=1)

    start_index = df_stock[df['Date'] == start].index.tolist()
    while not start_index:
        start = start + one_day
        start_index = df[df['Date'] == start].index.tolist()

    end_index = df[df['Date'] == end].index.tolist()
    while not end_index:
        end = end - one_day
        end_index = df[df['Date'] == end].index.tolist()

    return df.loc[start_index[0] : end_index[0]].set_index('Date')


# function to convert dates of typ sting to datetime format
def to_datetime(date_str):
    '''Utiliy function to convert strings to datetime format'''
    try:
        fmt = '%Y-%m-%d'
        date_datetime = datetime.date.strptime(date_str, fmt)
        return(date_datetime)

    except:
        return(date_str)


# main function to create dataframe containing daily yields and the daily sentiment
def polarity(dataframe, sent_dict, pol):
    '''Function to calculate the weighted average of positive / negative polarity depending on a user's followers count.
    !!! DEFAULT IS POSITIVE !!! For negative Polarity pol == False'''

    if pol == True:
        rows = dataframe.loc[dataframe[sent_dict] > 0]
    else:
        rows = dataframe.loc[dataframe[sent_dict] < 0]

    rows['pol_w'] = rows[sent_dict] * rows['user_followers']
    followers_count = rows['user_followers'].sum()
    polarity_mean_w = rows['sent_w'].sum() / followers_count

    return(polarity_mean_w)


def threshold_sentiment(df_sent, sent_dict, percentile):
    print("DF SENT FOR THRESHOLD:", len(df_sent))

    values_neg = []
    values_pos = []

    null_counter = 0

    for index, tweet in df_sent.iterrows():
        sent = tweet['{}'.format(sent_dict)]

        if sent < 0:
            values_neg.append(sent)
        elif sent > 0:
            values_pos.append(sent)
        else:
            null_counter = null_counter + 1
            #if (null_counter % 2) == 0:
            #    values_pos.append(sent)
            #else:
            #    values_neg.append(sent)

    tweets_count = len(values_neg + values_pos)

    try: ratio_neg = len(values_neg) / tweets_count
    except: ratio_neg = 0
    try: ratio_pos = len(values_pos) / tweets_count
    except: ratio_pos = 0

    zero_neg = round(ratio_neg * null_counter)
    zero_pos = round(ratio_pos * null_counter)
    values_neg = values_neg + ([0]*zero_neg)
    values_pos = values_pos + ([0]*zero_pos)

    try:
        sent_min_pos = np.percentile(values_pos, percentile)
    except:
        sent_min_pos = 0

    try:
        sent_min_neg = np.percentile(values_neg, (100 - percentile))
    except:
        sent_min_neg = 0

    return df_sent.loc[(df_sent[sent_dict] >= sent_min_pos) | (df_sent[sent_dict] <= (sent_min_neg))]


def threshold_tweetcount(c2c_sent, percentile):

    tc_min = np.percentile(c2c_sent['tweet_count_unfiltered'], percentile)
    df =  c2c_sent.loc[(c2c_sent['tweet_count_unfiltered'] >= tc_min)]
    df_threshed = c2c_sent.loc[(c2c_sent['tweet_count_unfiltered'] < tc_min)]

    return df


def close2close_sentiments(df_sent, sent_dict, df_stock, sent_mins, vol_mins, volume_filter, sentiment_filter):
    '''Untility function to create aggregatet sentiment metrics (c-2-c) and to merge them with daily stock quotes'''

    daily_sentiments = []
    dates = df_stock.index  # dates to search for in tweets dataframe

    for date in dates:

        sent_c2c = {}

        # adding column of weighted sentiment depending on the followers count
        print(sent_dict)
        df_sent['sent_w'] = df_sent[sent_dict] * df_sent['user_followers']

        today = date.to_datetime()

        sent_c2c['date'] = today

        # defining timedeltas select c2c-rows
        one_day = timedelta(days=1)
        two_days = timedelta(days=2)
        three_days = timedelta(days=3)

        df_sent.date = pd.to_datetime(df_sent.date)

        # group tweets to c-2-c
        if today.weekday()!=0:
            yesterday = today - one_day
            rows_today = df_sent.loc[(df_sent.date == today) & (df_sent.timeslot.isin(['during', 'before']))]
            rows_yesterday = df_sent.loc[(df_sent.date == yesterday) & (df_sent.timeslot == 'after')]
            rows = pd.concat([rows_today, rows_yesterday])

        else:
            weekend = [today - one_day, today - two_days]
            rows_today = df_sent.loc[(df_sent.date == today) & (df_sent.timeslot.isin(['during', 'before']))]
            rows_weekend = df_sent.loc[(df_sent.date.isin(weekend)) | ((df_sent.date == today - three_days) & (df_sent.timeslot == 'after'))]
            rows = pd.concat([rows_today, rows_weekend])

        sent_c2c['tweet_count_unfiltered'] = len(rows)

        if sentiment_filter:
            rows = threshold_sentiment(rows, sent_dict, sent_mins)

        else:
            rows = rows

        # calculating positive and negative polarity (weighted average)
        sent_c2c['pol_pos'] = polarity(rows, sent_dict, True)
        sent_c2c['pol_neg'] = polarity(rows, sent_dict, False)

        # calculating the c-2-c-sentiment
        sent_c2c['sent_mean'] = np.mean(rows[sent_dict])
        sent_c2c['sent_std'] = np.std(rows[sent_dict])

        # getting the number of tweets, positive tweets as well as negative tweets
        sent_c2c['tweet_count'] = len(rows)
        sent_c2c['tweet_count_w'] = rows['user_followers'].sum()
        sent_c2c['count_pos'] = len(rows.loc[rows[sent_dict] >= 0])
        sent_c2c['count_neg'] = len(rows.loc[rows[sent_dict] < 0])
        sent_c2c['count_pos_w'] = rows.loc[rows[sent_dict] >= 0]['user_followers'].sum()
        sent_c2c['count_neg_w'] = rows.loc[rows[sent_dict] < 0]['user_followers'].sum()

        # getting the ratio of positive and negative tweets
        try:
            sent_c2c['ratio_pos'] = (sent_c2c['count_pos'] / sent_c2c['tweet_count'])
            sent_c2c['ratio_neg'] = sent_c2c['count_neg'] / sent_c2c['tweet_count']
            sent_c2c['ratio_pos_w'] = sent_c2c['count_pos_w'] / sent_c2c['tweet_count_w']
            sent_c2c['ratio_neg_w'] = sent_c2c['count_neg_w'] / sent_c2c['tweet_count_w']
            sent_c2c['bullishness'] = np.log((1+sent_c2c['count_pos'])/(1+sent_c2c['count_neg']))
            sent_c2c['agreement'] = 1 - (1 - ((sent_c2c['count_pos'] - sent_c2c['count_neg'])/(sent_c2c['count_pos'] + sent_c2c['count_neg'])) ** 2) ** 0.5

        except Exception as e:
            print("Error calculating c2c-Sentiment metrics:", e)

        # calculating the c-2-c-sentiment depending on the followers
        sent_c2c['sent_mean_w'] = rows['sent_w'].sum() / sent_c2c['tweet_count_w']

        daily_sentiments.append(sent_c2c)

    if volume_filter:
        df_c2c = threshold_tweetcount(pd.DataFrame(daily_sentiments), vol_mins)
    else:
        df_c2c = pd.DataFrame(daily_sentiments)

    return(df_c2c.set_index('date'))


def sent_stock_corr(df_sentstock, corr_var_sent, corr_var_stock):
        return df_sentstock['{}'.format(corr_var_stock)].corr(df_sentstock['{}'.format(corr_var_sent)])


def main_correlation_stockwise(list_of_companies, list_of_dicts, list_of_corr_var_sent, corr_var_stock, sent_min, percentile_tweetcount, volume_filter, sentiment_filter):
    '''Main function to analyze the correlation between sentiments and stock prices.
    INPUT VARIABLES:
    1) a LIST of companies
    2) a sentiment dictionary
    3) a LIST of varibles we want to measure the correlation for (e.g. sent_mean, ratio_pos, sent_mean_w [...])'''

    for sentiment_dict in list_of_dicts:
        print(sentiment_dict)

        companies_list = [company.replace('$', '') for company in list_of_companies]
        correlations = []

        for company in companies_list:
                # open tweets
                df_tweets = open_df_sent(company)
                # calculate timeperiod to parse stock quotes
                start = date_start(df_tweets)
                end = date_end(df_tweets)
                # parse stock quotes
                df_stock = daily_yield(company, start, end)


                # get close to close sentiments
                df_c2cSent = close2close_sentiments(df_tweets, sentiment_dict, df_stock, sent_min, percentile_tweetcount, volume_filter, sentiment_filter)

                # crate correlation dataframe
                corr_SentYields = {}

                # define columns of correlation dataframe
                corr_SentYields['company'] = company
                corr_SentYields['sentiment_dict'] = sentiment_dict
                corr_SentYields['average_tweet_count'] = np.mean(df_c2cSent['tweet_count'])

                for corr_var_sent in list_of_corr_var_sent:
                    df_sentstock = pd.concat([df_c2cSent, df_stock], axis=1)
                    corr_SentYields['correlation_{}'.format(corr_var_sent)] = sent_stock_corr(df_sentstock, corr_var_sent, corr_var_stock)

                correlations.append(corr_SentYields)

        df_corr = pd.DataFrame(correlations).set_index('company')

        file_path = 'C:\\Users\\Open Account\\Documents\\BA_JonasIls\\Literatur & Analysen\\Correlations\\Stockwise\\20180303_Corr{}_{}_Vol{}_Sent{}.xls'.format(sentiment_dict, corr_var_stock, percentile_tweetcount, sent_min)
        df_corr.to_excel(file_path, encoding='utf-8')

        return(df_corr)


def main_correlation_allstocks(sentiment_dict, sent_min, vol_min):
    #df_sentstock = pd.read_csv('C:\\Users\\Open Account\\Documents\\BA_JonasIls\\Literatur & Analysen\\Correlations\\All_Stocks\\{}\\20180217_DF_C2C{}_{}vol_{}sen'.format(sentiment_dict, sentiment_dict, vol_min, sent_min))
    df_sentstock = pd.read_csv('C:\\Users\\Open Account\\Documents\\BA_JonasIls\\Literatur & Analysen\\Correlations\\All_Stocks\\SentimentGI\\20180217_DF_C2CSentimentGI_50vol_50sen.csv')

    correlations = df_sentstock.corr()

    file_path = 'C:\\Users\\Open Account\\Documents\\BA_JonasIls\\Literatur & Analysen\\Correlations\\20180217_CorrAllStocks{}_{}Vol_{}Sent.xls'.format(sentiment_dict, vol_min, sent_min)
    correlations.to_excel(file_path, encoding='uft-8')
    return correlations


def main_aggregate_sentiments(companies, sentiment_dictionary, sent_min):
    '''Main Function to return a dataframe analyzing c2c-Sentiments:
        1) amount of negative tweets
        2) amount of positive tweets
        3) weighted average of polarities
        4) weighted average of sentiment
        5) amount of tweets

        INPUT VARIABLES:
        1) LIST of companies
        2) LIST of sentiment dictionaries
        3) minimal sentiment for threshold function
        4) percentile to filter high volume days
        5) True if you want to filter days by weighted volume of tweets / False if you want to filter days by regular volume of tweets
        '''

    for company in companies:
        for sentiment_dict in sentiment_dicts:

            df_tweets = open_df_sent(company)

            start = date_start(df_tweets)
            end = date_end(df_tweets)
            df_stock = daily_yield(company, start, end)

            df_tweets = threshhold(df_tweets, sentiment_dict, sent_min)
            df_c2c = close2close_sentiments(df_tweets, sentiment_dictionary, df_stock)

            return(df_c2c)


def main_ct_analysis (sentiment_dictionary, percentile):

    companies = ['$MSFT', '$MMM', '$AXP', '$AAPL', '$BA', '$CAT', '$CVX', '$CSCO', '$KO', '$DWDP', '$DIS', '$XOM',
                 '$GE', '$GS', '$HD', '$IBM', '$INTC', '$JNJ', '$JPM', '$MCD', '$MRK', '$NKE', '$PFE', '$PG', '$TRV',
                 '$UTX', '$UNH', '$VZ', '$V', '$WMT']
    companies = [company.replace('$', '') for company in companies]


    # Define aggregated sentiment metrics you'd like to analyze
    corr_var_sent = ['ratio_neg', 'ratio_neg_w', 'ratio_pos', 'ratio_pos_w', 'sent_mean', 'sent_mean_w', 'bullishness']
    # Define ONE stock quote you'd  like to analyze
    corr_var_stock = 'abnormal_returns'

    # Define sentiment minimum you'd like to analyze
    sent_min = sent_min

    # Define percentile of volume you'd like to analyze and declare if you'd like to calculate the weighted volume

    filter_off = main_correlation_allstocks(list_of_companies=companies, sentiment_dicts=sentiment_dictionary,
                                                  corr_var_sent=corr_var_sent, corr_var_stock=corr_var_stock,
                                                  sent_min=percentile, percentile_tweetcount=percentile,
                                                  volume_filter=False, sentiment_filter=False)

    filter_sentiment_on = main_correlation_allstocks(list_of_companies=companies, sentiment_dicts=sentiment_dictionary,
                                                  corr_var_sent=corr_var_sent, corr_var_stock=corr_var_stock,
                                                  sent_min=percentile, percentile_tweetcount=percentile,
                                                  volume_filter=False, sentiment_filter=True)

    filter_volume_on = main_correlation_allstocks(list_of_companies=companies, sentiment_dicts=sentiment_dictionary,
                                                  corr_var_sent=corr_var_sent, corr_var_stock=corr_var_stock,
                                                  sent_min=percentile, percentile_tweetcount=percentile,
                                                  volume_filter=True, sentiment_filter=False)

    filter_on = main_correlation_allstocks(list_of_companies=companies, sentiment_dicts=sentiment_dictionary,
                                            corr_var_sent=corr_var_sent, corr_var_stock=corr_var_stock,
                                            sent_min=percentile, percentile_tweetcount=percentile,
                                            volume_filter=True, sentiment_filter=True)

    df = pd.concat([filter_off, filter_sentiment_on, filter_volume_on, filter_on])
    df['Mode'] = np.asarray(['no_filter', 'sentiment_filter', 'volume_filter', 'both'])

    df.to_excel('C:\\Users\\Open Account\\Documents\\BA_JonasIls\\Literatur & Analysen\\Correlations\\24012018_CP_{}{}.xls'.format(sentiment_dictionary, sent_min))

    return df


def main(corr_var_stock, corr_var_sent, list_of_companies, sent_mins, vol_mins, sentiment_dict, write):
    raw_data = []
    for company in list_of_companies:
        print('Open {}'.format(company))
        df_tweets = open_df_sent(company)
        # calculate timeperiod to parse stock quotes
        start = date_start(df_tweets)
        end = date_end(df_tweets)
        # parse stock quotes
        df_stock = daily_yield(company, start, end)

        tuple = (df_tweets, df_stock)

        raw_data.append(tuple)
        print('Append {}'.format(company))

    byfilter_corr = []
    byfilter_tweets = []
    byfilter_days = []

    for sent_min in sent_mins:
        corr_SentYields = {}
        tweets_count = {}
        days_count = {}

        corr_SentYields['sentiment_dict'] = sentiment_dict
        tweets_count['sentiment_dict'] = sentiment_dict
        days_count['sentiment_dict'] = sentiment_dict

        corr_SentYields['sent_min'] = sent_min
        tweets_count['sent_min'] = sent_min
        days_count['sent_min'] = sent_min

        for vol_min in vol_mins:

            dataframes_c2c = []
            for tuple in raw_data:
                df_sent = tuple[0]
                df_stock = tuple[1]

                #try:
                    # get close to close sentiments
                df_c2cSent = close2close_sentiments(df_sent, sentiment_dict, df_stock, sent_min, vol_min, volume_filter=True, sentiment_filter=True)
                df_sentstock = pd.concat([df_c2cSent, df_stock], axis=1)
                dataframes_c2c.append(df_sentstock)

                #except:
                'Fuck Off'

            df_DJI = pd.concat(dataframes_c2c)
            df_DJI.to_csv('C:\\Users\\Open Account\\Documents\\BA_JonasIls\\Literatur & Analysen\\Correlations\\All_Stocks\\20180217_DF_C2C{}_{}vol_{}sen.csv'.format(sentiment_dict, vol_min, sent_min), encoding='utf-8')

            corr_SentYields['{}'.format(vol_min)] = sent_stock_corr(df_DJI, corr_var_sent, corr_var_stock)
            tc = df_DJI['tweet_count'].sum()
            fc = df_DJI['tweet_count_w'].sum()
            tweets_count['{}'.format(vol_min)] = '{}/{}'.format(tc, fc)
            days_count['{}'.format(vol_min)] = len(df_DJI)

        byfilter_corr.append(corr_SentYields)
        byfilter_tweets.append(tweets_count)
        byfilter_days.append(days_count)

    heatmap_corr = pd.DataFrame(byfilter_corr)
    heatmap_tweets = pd.DataFrame(byfilter_tweets)
    heatmap_days = pd.DataFrame(byfilter_days)

    if write == True:
        heatmap_corr.to_excel('C:\\Users\\Open Account\\Documents\\BA_JonasIls\\Literatur & Analysen\\Correlations\\Heat_Maps\\20180217_HMCorr_{}_{}.xls'.format(corr_var_stock, corr_var_sent), encoding='uft-8')
        heatmap_tweets.to_excel('C:\\Users\\Open Account\\Documents\\BA_JonasIls\\Literatur & Analysen\\Correlations\\Heat_Maps\\20180217_HMTweets_{}_{}.xls'.format(corr_var_stock, corr_var_sent), encoding='uft-8')
        heatmap_days.to_excel('C:\\Users\\Open Account\\Documents\\BA_JonasIls\\Literatur & Analysen\\Correlations\\Heat_Maps\\20180217_HMDays_{}_{}.xls'.format(corr_var_stock, corr_var_sent), encoding='uft-8')

    else: 'Do Nothing'

    return(heatmap_corr)

if __name__ == "__main__":
    df_index = get_df_index('DJI')

    LM = 'SentimentLM'
    GI = 'SentimentGI'
    HE = 'SentimentHE'
    TB = 'SentimentTB'

    list_of_dicts = [GI]

    # Define companies you'd like to analyze
    companies = ['$MSFT', '$MMM', '$AXP', '$BA', '$CAT', '$CVX', '$CSCO', '$KO', '$DWDP', '$DIS', '$XOM', '$GS', '$HD', '$IBM', '$INTC', '$JNJ', '$JPM', '$MCD', '$MRK', '$NKE', '$PFE', '$PG', '$TRV', '$UTX', '$UNH', '$VZ', '$V', '$WMT']
    #companies = ['$MSFT', '$MMM', '$AXP', '$BA', '$CAT', '$CVX', '$CSCO', '$KO', '$DWDP', '$DIS', '$XOM',
    #             '$GE', '$GS', '$HD', '$IBM', '$INTC', '$JNJ', '$JPM', '$MCD', '$MRK', '$NKE', '$PFE', '$PG', '$TRV',
    #             '$UTX', '$UNH', '$VZ', '$V', '$WMT']

    companies = [company.replace('$', '') for company in companies]

    corr_var_stock = 'volatility_parks'
    corr_var_sent = 'agreement'

    filter = [0, 25, 50, 75]

    #heat_map = main(corr_var_stock=corr_var_stock, corr_var_sent=corr_var_sent, list_of_companies=companies, sent_mins=filter, vol_mins=filter, sentiment_dict=GI, write=True)
    #print(heat_map)

    main_correlation_allstocks(GI,  50, 50)

    #corr_matrix = main_correlation_stockwise(companies, list_of_dicts, [corr_var_sent], corr_var_stock, 0, 50, True, True)
    #print(corr_matrix)

