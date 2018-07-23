import pandas as pd
import numpy as np
from datetime import datetime, timedelta, date
import statsmodels.api as sm
import math


def get_TBSentiment(text):
    '''Function to calculate a sentiment score based on the textblob library'''
    analysis = TextBlob(text)
    SentimentTB = analysis.sentiment.polarity
    return(SentimentTB)


def open_df_sent(company):
    file_path = 'C:\\Users\\jonas\\Documents\\BA_JonasIls\\Twitter_Streaming\\Sentiment_Dataframes\\20180101_20180410\\20180101_20180410_SentimentDataframes_{}'.format(company)
    df_tweets = pd.read_csv(file_path, encoding="utf-8")
    df_tweets = df_tweets

    df_tweets = df_tweets.fillna(0)
    df_tweets['text_clean'] = df_tweets['text_clean'].astype(str)
    df_tweets['date'] = pd.to_datetime(df_tweets['date'], errors='coerce')
    #print(len(df_tweets))
    df_tweets = df_tweets.loc[df_tweets['date'] != 'NaT']
    #print(len(df_tweets))
    df_tweets['user_followers'] = df_tweets['user_followers'].astype(int)

    # adding sentiment calculated with textblob
    #df_tweets['SentimentTB'] = df_tweets['text_clean'].apply(get_TBSentiment)

    return df_tweets


def date_start(df_sent):
    return(df_sent.date.iloc[0])


def date_end(df_sent):
    return(df_sent.date.iloc[len(df_sent)-1])


def get_df_index(index):
    df_index = pd.read_csv('C:\\Users\\jonas\\Documents\\BA_JonasIls\\Stock_Quotes\\20180613StockPrices_{}.csv'.format(index), encoding='utf-8')
    df_index['daily_returns_index'] = df_index['Close'] / df_index['Close'].shift(1) - 1
    return df_index


def daily_yield(company, start, end):
    '''Function to parse daily stock quotes and calculate daily yields'''

    # parsing stockdata
    df_stock = pd.read_csv('C:\\Users\\jonas\\Documents\\BA_JonasIls\\Stock_Quotes\\20180613StockPrices_{}.csv'.format(company))


    # calculating yields
    df_stock['daily_returns'] = df_stock['Close']/df_stock['Close'].shift(1)-1
    df_stock['daily_gap'] = df_stock['Close']/df_stock['Close'].shift(1)-1
    df_index = get_df_index('DIA')

    # contact dataframes
    df = pd.concat([df_stock, df_index['daily_returns_index']], axis=1)

    # calculating abnormal returns
    '''Alternative One: Calculating ols regressed abnormal returns manually over variances and covariance of the variables'''
    #df.rolling_cov100 = pd.rolling_cov(arg1=df.daily_returns, arg2=df.daily_returns_index, window=100, min_periods=100)
    #df.rolling_varIndex100 = pd.rolling_var(df.daily_returns_index, window=100)
    #beta = df.rolling_cov100 / df.rolling_varIndex100
    #df['abnormal_returns'] = df['daily_returns'] - df['daily_returns_index'] * beta

    '''Alternative Two: Calculating ols regressed abnormal returns over the rolling ols module of pandas'''
    #model = pd.stats.ols.MovingOLS(y=df.daily_returns, x=df.daily_returns_index,
    #                               window_type='rolling', window=100, intercept=True)
    #returns_est = model.y_predict
    #df['abnormal_returns'] = df['daily_returns'] - returns_est

    '''Alternative Three: since these freakin dickheads seem to depricate every fucking method that ever made pandas a great module
    we have to use a much simpler and also quite shitty approach of calculating abnormal returns. No ols regressed abnormal return
    just a simple subtraction of the daily index return. FUCK OFF, WHY THE HACK ONE WEEK BEFORE I HAVE TO FUCKING FINISH THIS THESIS?
    WHY?'''
    df['abnormal_returns'] = df['daily_returns'] - df['daily_returns_index']

    #calculating parks volatility
    df['volatility_parks'] = ((np.log(df['High']-np.log(df['Low'])))**2) / (4 * np.log(2))
    df['volume_dollar'] = df['Volume'] * ((df['Open']+df['Close'])/2)
    rolling_mean = df['volume_dollar'].rolling(window=100).mean()
    rolling_std = df['volume_dollar'].rolling(window=100).std()
    df['volume_std'] = (df['volume_dollar'] - rolling_mean) / rolling_std

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


def weekly_return(company):
    df_stock = pd.read_csv('C:\\Users\\jonas\\Documents\\BA_JonasIls\\Stock_Quotes\\20180613StockPrices_{}.csv'.format(company))
    df_index = get_df_index('DIA')
    df_stock['Close_Index'] = df_index['Close']

    avg_price = (df_stock['High'] + df_stock['Low']) / 2
    vol_dollar = df_stock['Volume'] * avg_price
    vol_weekly = vol_dollar.rolling(window=7).sum()
    df_stock['vol_weekly_std'] = (vol_weekly - vol_weekly.mean())/vol_weekly.std()

    df_stock['Date'] = pd.to_datetime(df_stock ['Date'])
    df_stock['weekday'] = df_stock ['Date'].dt.dayofweek
    df_stock['cw'] = df_stock ['Date'].dt.week
    df_stock['year'] = df_stock['Date'].dt.year
    years = df_stock.year.unique()

    # select last day of week, taking possible holidays on a friday into account
    rows = []

    for year in years:
        for week in df_stock.cw.unique():
            d = df_stock.loc[(df_stock.cw == week) & (df_stock.year == year)]
            try:
                d = d.loc[d.weekday == max(d.weekday)]
            except:
                print('max() is empty since week is not available for the respective year')
            rows.append(d)

    df_stock = pd.concat(rows)

    #simple solution of getting close-days of weeks // PROBLEM if Friday is holiday!!
    #df_stock = df_stock .loc[df_stock['weekday'] == 4]

    #df_index = get_df_index('DIA')
    #df_index['Date'] = pd.to_datetime(df_index['Date'])
    #df_index['weekday'] = df_index['Date'].dt.dayofweek
    #df_index = df_index.loc[df_index['weekday'] == 4]
    #df_index['cw'] = df_index['Date'].dt.week

    df_stock['weekly_returns'] = df_stock['Close']/df_stock['Close'].shift(1)-1
    df_stock['weekly_returns_index'] = df_stock['Close_Index']/df_stock['Close_Index'].shift(1)-1
    df_stock['abnormal_returns'] = df_stock['weekly_returns'] - df_stock['weekly_returns_index']

    return df_stock


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
        else:
            null_counter = null_counter + 1
            #if (null_counter % 2) == 0:
            #    values_pos.append(sent)
            #else:
            #    values_neg.append(sent)

    tweets_count = len(values_neg + values_pos)

    # allocating 0-Sentiments based on the ratio of pos/neg tweets
    try: ratio_neg = len(values_neg) / tweets_count
    except: ratio_neg = 0
    try: ratio_pos = len(values_pos) / tweets_count
    except: ratio_pos = 0
    zero_neg = round(ratio_neg * null_counter)
    zero_pos = round(ratio_pos * null_counter)
    values_neg = values_neg + ([0]*zero_neg)
    values_pos = values_pos + ([0]*zero_pos)

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


def threshold_tweetcount(c2c_sent, percentile):

    tc_min = np.percentile(c2c_sent['tweet_count_unfiltered'], percentile)
    df =  c2c_sent.loc[(c2c_sent['tweet_count_unfiltered'] >= tc_min)]
    df_threshed = c2c_sent.loc[(c2c_sent['tweet_count_unfiltered'] < tc_min)]

    return df


def close2close_sentiments(df_sent, sent_dict, df_stock, sent_mins, vol_mins, volume_filter, sentiment_filter):
    '''Utility function to create aggregatet sentiment metrics (c-2-c) and to merge them with daily stock quotes'''

    daily_sentiments = []
    df_stock.index = pd.to_datetime(df_stock.index)
    dates = df_stock.index  # dates to search for in tweets dataframe
    holidays = [date(2018, 2, 20), date(2018, 1, 16), date(2018, 3, 30)]

    for today in dates:

        #dictionary to store aggregated metrics
        sent_c2c = {}

        # adding column of weighted sentiment depending on the followers count
        df_sent['sent_w'] = df_sent[sent_dict] * df_sent['user_followers']
        sent_c2c['date'] = today

        # defining timedeltas select c2c-rows
        one_day = timedelta(days=1)
        two_days = timedelta(days=2)
        three_days = timedelta(days=3)
        four_days = timedelta(days=4)

        df_sent.date = pd.to_datetime(df_sent.date)

        # group tweets to c-2-c
        if today.weekday()!=0:
            yesterday = today - one_day
            rows_before = df_sent.loc[(df_sent.date == today) & (df_sent.timeslot == 'before')]
            rows_during = df_sent.loc[(df_sent.date == today) & (df_sent.timeslot == 'during')]
            rows_after = df_sent.loc[(df_sent.date == yesterday) & (df_sent.timeslot == 'after')]
            rows = pd.concat([rows_before, rows_during, rows_after])

        else:
            weekend = [today - one_day, today - two_days]
            rows_before = df_sent.loc[(df_sent.date == today) & (df_sent.timeslot == 'before')]
            rows_during = df_sent.loc[(df_sent.date == today) & (df_sent.timeslot == 'during')]
            rows_after = df_sent.loc[(df_sent.date.isin(weekend)) | ((df_sent.date == today - three_days) & (df_sent.timeslot == 'after'))]
            rows = pd.concat([rows_before, rows_during, rows_after])

        sent_c2c['tweet_count_unfiltered'] = len(rows)

        if sentiment_filter:
            rows = threshold_sentiment(rows, sent_dict, sent_mins)
        else:
            rows = rows

        # calculating positive and negative polarity (weighted average)
        #sent_c2c['pol_pos'] = polarity(rows, sent_dict, True)
        #sent_c2c['pol_neg'] = polarity(rows, sent_dict, False)

        # calculating the c-2-c-sentiment
        sent_c2c['sent_mean'] = np.mean(rows[sent_dict])
        sent_c2c['sent_std'] = np.std(rows[sent_dict])

        # getting the number of tweets, positive tweets as well as negative tweets,
        # for the whole day and for each time slot
        sent_c2c['tweet_count'] = len(rows)
        sent_c2c['tweet_count_w'] = rows['user_followers'].sum()
        sent_c2c['tweet_count_w_b'] = rows_before['user_followers'].sum()
        sent_c2c['tweet_count_w_d'] = rows_during['user_followers'].sum()
        sent_c2c['tweet_count_w_a'] = rows_after['user_followers'].sum()
        sent_c2c['count_pos'] = len(rows.loc[rows[sent_dict] > 0])
        sent_c2c['count_neg'] = len(rows.loc[rows[sent_dict] < 0])
        sent_c2c['count_pos_w'] = rows.loc[rows[sent_dict] > 0]['user_followers'].sum()

        sent_c2c['count_pos_w_d'] = rows_during.loc[rows_during[sent_dict] > 0]['user_followers'].sum()
        sent_c2c['count_pos_w_a'] = rows_after.loc[rows_after[sent_dict] > 0]['user_followers'].sum()
        sent_c2c['count_pos_w_b'] = rows_before.loc[rows_before[sent_dict] > 0]['user_followers'].sum()
        sent_c2c['count_neg_w'] = rows.loc[rows[sent_dict] < 0]['user_followers'].sum()
        sent_c2c['count_neg_w_d'] = rows_during.loc[rows_during[sent_dict] < 0]['user_followers'].sum()
        sent_c2c['count_neg_w_a'] = rows_after.loc[rows_after[sent_dict] < 0]['user_followers'].sum()
        sent_c2c['count_neg_w_b'] = rows_before.loc[rows_before[sent_dict] < 0]['user_followers'].sum()

        sent_c2c['count_pos_b'] = len(rows_before.loc[rows_before[sent_dict] > 0])
        sent_c2c['count_neg_b'] = len(rows_before.loc[rows_before[sent_dict] < 0])
        sent_c2c['count_pos_a'] = len(rows_after.loc[rows_after[sent_dict] > 0])
        sent_c2c['count_neg_a'] = len(rows_after.loc[rows_after[sent_dict] < 0])
        sent_c2c['count_pos_d'] = len(rows_during.loc[rows_during[sent_dict] > 0])
        sent_c2c['count_neg_d'] = len(rows_during.loc[rows_during[sent_dict] < 0])

        # getting the ratio of positive and negative tweets
        try:
            #sent_c2c['ratio_pos'] = (sent_c2c['count_pos'] / sent_c2c['tweet_count'])
            #sent_c2c['ratio_neg'] = sent_c2c['count_neg'] / sent_c2c['tweet_count']
            #sent_c2c['ratio_pos_w'] = sent_c2c['count_pos_w'] / sent_c2c['tweet_count_w']
            #sent_c2c['ratio_neg_w'] = sent_c2c['count_neg_w'] / sent_c2c['tweet_count_w']
            sent_c2c['bullishness'] = np.log((1 + sent_c2c['count_pos']) / (1 + sent_c2c['count_neg']))
            sent_c2c['bullishness_w'] = np.log((1+sent_c2c['count_pos_w'])/(1+sent_c2c['count_neg_w']))
            sent_c2c['bullishness_w_d'] = np.log((1+sent_c2c['count_pos_w_d'])/(1+sent_c2c['count_neg_w_d']))
            sent_c2c['bullishness_w_a'] = np.log((1 + sent_c2c['count_pos_w_a']) / (1 + sent_c2c['count_neg_w_a']))
            sent_c2c['bullishness_w_b'] = np.log((1 + sent_c2c['count_pos_w_b']) / (1 + sent_c2c['count_neg_w_b']))
            sent_c2c['bullishness_b'] = np.log((1+sent_c2c['count_pos_b'])/(1+sent_c2c['count_neg_b']))
            sent_c2c['bullishness_a'] = np.log((1+sent_c2c['count_pos_a'])/(1+sent_c2c['count_neg_a']))
            sent_c2c['bullishness_d'] = np.log((1+sent_c2c['count_pos_d'])/(1+sent_c2c['count_neg_d']))

            sent_c2c['agreement'] = 1 - (1 - ((sent_c2c['count_pos'] - sent_c2c['count_neg'])/(sent_c2c['count_pos'] + sent_c2c['count_neg'])) ** 2) ** 0.5

        except Exception as e:
            print("Error calculating c2c-Sentiment metrics:", e)

        # calculating the c-2-c-sentiment depending on the followers
        sent_c2c['sent_mean_w'] = rows['sent_w'].sum() / sent_c2c['tweet_count_w']
        sent_c2c['sent_mean_w_b'] = rows_before['sent_w'].sum() / sent_c2c['tweet_count_w_b']
        sent_c2c['sent_mean_w_d'] = rows_during['sent_w'].sum() / sent_c2c['tweet_count_w_d']
        sent_c2c['sent_mean_w_a'] = rows_after['sent_w'].sum() / sent_c2c['tweet_count_w_a']

        daily_sentiments.append(sent_c2c)

    if volume_filter:
        df_c2c = threshold_tweetcount(pd.DataFrame(daily_sentiments), vol_mins)
    else:
        df_c2c = pd.DataFrame(daily_sentiments)

    var_std = ['tweet_count', 'count_pos', 'count_neg']

    for x in var_std:
        mean = df_c2c['{}'.format(x)].mean()
        std = df_c2c['{}'.format(x)].std()
        df_c2c['{}_std'.format(x)] = ((df_c2c['{}'.format(x)]-mean) / std)


    df_c2c = df_c2c.reset_index()
    df_c2c = df_c2c.set_index('date')

    return df_c2c.dropna(subset=['bullishness'])


def sent_stock_corr(df_sentstock, corr_var_sent, corr_var_stock):
        return df_sentstock['{}'.format(corr_var_stock)].corr(df_sentstock['{}'.format(corr_var_sent)])


def main_correlation_stockwise(list_of_companies, list_of_dicts, list_of_corr_var_sent, corr_var_stock, sent_min, percentile_tweetcount, volume_filter, sentiment_filter):
    '''Main function to analyze the correlation between sentiments and stock prices.
    INPUT VARIABLES:
    1) a LIST of companies
    2) a sentiment dictionary
    3) a LIST of varibles we want to measure the correlation for (e.g. sent_mean, ratio_pos, sent_mean_w [...])'''

    companies_list = [company.replace('$', '') for company in list_of_companies]

    for sentiment_dict in list_of_dicts:

        correlations = []

        for company in companies_list:
                print(company, sentiment_dict)

                # open tweets
                df_tweets = open_df_sent(company)
                # calculate timeperiod to parse stock quotes
                start = date_start(df_tweets)
                end = date_end(df_tweets)
                # parse stock quotes
                df_stock = daily_yield(company, start, end)

                bad_days = ['2018-01-04', '2018-01-03', '2018-01-02', '2018-01-01', '2018-01-25', '2018-01-26',
                            '2018-01-27', '2018-02-02', '2018-02-03', '2018-02-06', '2018-02-10', '2018-03-29',
                            '2018-04-01', '2018-04-10']

                # deleting gap in stream from 20th Feb. till the 16th of March
                d1 = date(2018, 2, 20)  # start date
                d2 = date(2018, 3, 16)  # end date
                delta = d2 - d1  # timedelta

                for i in range(delta.days + 1):
                    bad_days.append(str(d1 + timedelta(i)))

                df_stock.index = df_stock.index.astype(str)

                for bad_day in bad_days:
                    try:
                        df_stock = df_stock.drop(bad_day)
                    except Exception as e:
                        print(e)

                # get close to close sentiments
                df_c2cSent = close2close_sentiments(df_tweets, sentiment_dict, df_stock, sent_min, percentile_tweetcount, volume_filter, sentiment_filter)
                df_c2cSent = pd.concat([df_c2cSent, df_stock], axis=1)
                df_c2cSent.to_csv('C:\\Users\\jonas\\Documents\\BA_JonasIls\\Twitter_Streaming\\Sentiment_Dataframes\\20180101_20180410\\c2c_dataframes\\c2c_20180101_20180410{}_{}_Vol{}_Sent{}'.format(company,sentiment_dict, percentile_tweetcount, sent_min), encoding='utf-8', index_label='date')

                # crate correlation dataframe
                corr_SentYields = {}


                # define columns of correlation dataframe
                corr_SentYields['company'] = company
                corr_SentYields['sentiment_dict'] = sentiment_dict
                corr_SentYields['average_tweet_count'] = np.mean(df_c2cSent['tweet_count'])

                for corr_var_sent in list_of_corr_var_sent:
                    corr_SentYields['correlation_{}'.format(corr_var_sent)] = sent_stock_corr(df_c2cSent, corr_var_sent, corr_var_stock)

                correlations.append(corr_SentYields)

        df_corr = pd.DataFrame(correlations).set_index('company')

        file_path = 'C:\\Users\\jonas\\Documents\\BA_JonasIls\\Literatur & Analysen\\Correlations\\Stockwise\\20180101_20180410_Corr{}_{}_Vol{}_Sent{}.xls'.format(sentiment_dict, corr_var_stock, percentile_tweetcount, sent_min)
        df_corr.to_excel(file_path, encoding='utf-8')


def main_correlation_weekly(company, sent_dict):
    #open df sent
    df_sent = open_df_sent(company)

    #c2c aggregation on weekly basis
    df_sent['date'] = pd.to_datetime(df_sent['date'])
    df_sent['weekday'] = df_sent['date'].dt.weekday
    df_sent['cw'] = df_sent['date'].dt.week

    #define relevant weeks (week one and 15 are dropped, since there is only one day of data for this week
    weeks = df_sent.cw.unique()
    weeks = [x for x in weeks if x not in [1, 15]]

    weekly = []
    for week in weeks:
        print(week)
        # select tweets to extract for each week
        rows_during = df_sent.loc[(df_sent['cw'] == week) & (df_sent['weekday'].isin([0, 1, 2, 3, 4])) & (df_sent['timeslot'] == 'during')]
        rows_before = df_sent.loc[(df_sent['cw'] == week) & (df_sent['weekday'].isin([0, 1, 2, 3, 4])) & (df_sent['timeslot'] == 'before')]
        rows_after = df_sent.loc[(((df_sent['cw'] == week) & (df_sent['weekday'].isin([0, 1, 2, 3]))) |
                                  ((df_sent['cw'] == week-1) & (df_sent['weekday'] == 4))) &
                                 (df_sent['timeslot'] == 'after')]
        rows_weekend = df_sent.loc[(df_sent['cw'] == week) & (df_sent['weekday'].isin([5, 6]))]


        rows = df_sent.loc[((df_sent['cw'] == week) & ((df_sent['weekday'].isin([0, 1, 2, 3])))) |
                            ((df_sent['cw'] == week) &((df_sent['weekday'] == 4) & (df_sent['timeslot'].isin(['before', 'during'])))) |
                           ((df_sent['cw'] == week - 1) & (df_sent['weekday'].isin([5, 6]))) |
                            ((df_sent['cw'] == week - 1) & ((df_sent['weekday'] == 4) & (df_sent['timeslot'] == 'after')))]

        rows['sent_w'] = rows['{}'.format(sent_dict)]*rows['user_followers']

        # create dictionary to store aggregated sentiment metrics
        sent_c2c = {}

        sent_c2c['cw'] = week

        # calculating volume metrics
        sent_c2c['tweet_count'] = len(rows)
        sent_c2c['tweet_count_w'] = rows['user_followers'].sum()

        sent_c2c['count_pos'] = len(rows.loc[rows[sent_dict] > 0])
        sent_c2c['count_pos_d'] = len(rows_during.loc[rows_during[sent_dict] > 0])
        sent_c2c['count_pos_we'] = len(rows_weekend.loc[rows_weekend[sent_dict] > 0])

        sent_c2c['count_neg'] = len(rows.loc[rows[sent_dict] < 0])
        sent_c2c['count_neg_d'] = len(rows_during.loc[rows_during[sent_dict] < 0])
        sent_c2c['count_neg_we'] = len(rows_weekend.loc[rows_weekend[sent_dict] < 0])

        sent_c2c['count_pos_w'] = rows.loc[rows[sent_dict] > 0]['user_followers'].sum()
        sent_c2c['count_neg_w'] = rows.loc[rows[sent_dict] < 0]['user_followers'].sum()

        # calculating return an volatility metrics
        sent_c2c['sent_mean'] = np.mean(rows[sent_dict])
        sent_c2c['sent_std'] = np.std(rows[sent_dict])
        sent_c2c['sent_mean_w'] = rows['sent_w'].sum() / sent_c2c['tweet_count_w']
        sent_c2c['bullishness'] = np.log((1 + sent_c2c['count_pos']) / (1 + sent_c2c['count_neg']))
        sent_c2c['bullishness_d'] = np.log((1 + sent_c2c['count_pos_d']) / (1 + sent_c2c['count_neg_d']))
        sent_c2c['bullishness_we'] = np.log((1 + sent_c2c['count_pos_we']) / (1 + sent_c2c['count_neg_we']))

        sent_c2c['bullishness_w'] = np.log((1 + sent_c2c['count_pos_w']) / (1 + sent_c2c['count_neg_w']))
        sent_c2c['pol_pos'] = polarity(rows, sent_dict, True)
        sent_c2c['pol_neg'] = polarity(rows, sent_dict, False)
        try:
            sent_c2c['ratio_pos'] = (sent_c2c['count_pos'] / sent_c2c['tweet_count'])
            sent_c2c['ratio_neg'] = sent_c2c['count_neg'] / sent_c2c['tweet_count']
            sent_c2c['ratio_pos_w'] = sent_c2c['count_pos_w'] / sent_c2c['tweet_count_w']
            sent_c2c['ratio_neg_w'] = sent_c2c['count_neg_w'] / sent_c2c['tweet_count_w']
            sent_c2c['agreement'] = 1 - (1 - ((sent_c2c['count_pos'] - sent_c2c['count_neg']) / (sent_c2c['count_pos'] + sent_c2c['count_neg'])) ** 2) ** 0.5
        except Exception as e:
            print("Error calculating c2c-Sentiment metrics:", e)

        weekly.append(sent_c2c)

    df_sent = pd.DataFrame(weekly).set_index('cw')

    tc_mean = df_sent['tweet_count'].mean()
    tc_std = df_sent['tweet_count'].std()
    df_sent['tweet_count_std'] = (df_sent['tweet_count'] - tc_mean) / tc_std

    tc_mean = df_sent['tweet_count_w'].mean()
    tc_std = df_sent['tweet_count_w'].std()
    df_sent['tweet_count_w_std'] = (df_sent['tweet_count_w'] - tc_mean) / tc_std

    df_stock = weekly_return(company)
    df_stock = df_stock.loc[(df_stock.cw.isin(df_sent.index)) & (df_stock.Date.dt.year == 2018)].set_index('cw')

    return pd.concat([df_sent, df_stock], axis=1)


def main_correlation_allstocks(sentiment_dict, vol_min, sent_min, method):
    #df_sentstock = pd.read_csv('C:\\Users\\jonas\\Documents\\BA_JonasIls\\Literatur & Analysen\\Correlations\\All_Stocks\\{}\\20180217_DF_C2C{}_{}vol_{}sen'.format(sentiment_dict, sentiment_dict, vol_min, sent_min))
    df_sentstock = pd.read_csv('C:\\Users\\jonas\\Documents\\BA_JonasIls\\Twitter_Streaming\\Sentiment_Dataframes\\20180101_20180410\\C2C_Dataframes\\c2c_20180101_20180410AllStocks_{}_Vol{}_Sent{}'.format(sentiment_dict, vol_min, sent_min))
    #df_sentstock = pd.read_csv('C:\\Users\\jonas\\Documents\\BA_JonasIls\\Twitter_Streaming\\Sentiment_Dataframes\\spam_cleaned\\c2c_dataframes\\')

    correlations = df_sentstock.corr(method=method)

    file_path = 'C:\\Users\\jonas\\Documents\\BA_JonasIls\\Literatur & Analysen\\Correlations\\20180410_Corr{}AllStocks{}_{}Vol_{}Sent.xls'.format(method, sentiment_dict, vol_min, sent_min)
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

    df.to_excel('C:\\Users\\jonas\\Documents\\BA_JonasIls\\Literatur & Analysen\\Correlations\\24012018_CP_{}{}.xls'.format(sentiment_dictionary, sent_min))

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

        bad_days = ['2018-01-04', '2018-01-03', '2018-01-02', '2018-01-01', '2018-01-25', '2018-01-26',
                    '2018-01-27', '2018-02-02', '2018-02-03', '2018-02-06', '2018-02-10', '2018-03-29',
                    '2018-04-01', '2018-04-10']

        # deleting gap in stream from 20th Feb. till the 16th of March
        d1 = date(2018, 2, 20)  # start date
        d2 = date(2018, 3, 16)  # end date
        delta = d2 - d1  # timedelta

        for i in range(delta.days + 1):
            bad_days.append(str(d1 + timedelta(i)))

        df_stock.index = df_stock.index.astype(str)
        print(len(df_stock))

        for bad_day in bad_days:
            try:
                df_stock = df_stock.drop(bad_day)
            except Exception as e:
                print(e)

        tuple = (df_tweets, df_stock, company)

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
                print(tuple[2], ': ', vol_min, ' & ', sent_min)
                df_sent = tuple[0]
                df_stock = tuple[1]

                #try:
                    # get close to close sentiments
                df_c2cSent = close2close_sentiments(df_sent, sentiment_dict, df_stock, sent_min, vol_min, volume_filter=True, sentiment_filter=True)
                df_sentstock = pd.concat([df_c2cSent, df_stock], axis=1)
                #df_sentstock.to_csv('C:\\Users\\jonas\\Documents\\BA_JonasIls\\Twitter_Streaming\\Sentiment_Dataframes\\spam_cleaned\\C2C_Dataframes\\c2c_spamcleaned{}_{}_Vol{}_Sent{}.csv'.format(tuple[2], sentiment_dict, vol_min, sent_min), encoding='utf-8')
                dataframes_c2c.append(df_sentstock)

                #except:
                'Fuck Off'

            df_DJI = pd.concat(dataframes_c2c)
            #df_DJI.to_csv('C:\\Users\\jonas\\Documents\\BA_JonasIls\\Twitter_Streaming\\Sentiment_Dataframes\\spam_cleaned\\C2C_Dataframes\\c2c_spamcleanedAllStocks_{}_Vol{}_Sent{}.csv'.format(sentiment_dict, vol_min, sent_min), encoding='utf-8')

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
        heatmap_corr.to_excel('C:\\Users\\jonas\\Documents\\BA_JonasIls\\Literatur & Analysen\\Correlations\\Heat_Maps\\20180410{}_HMCorr_{}_{}.xls'.format(sentiment_dict, corr_var_stock, corr_var_sent), encoding='uft-8')
        heatmap_tweets.to_excel('C:\\Users\\jonas\\Documents\\BA_JonasIls\\Literatur & Analysen\\Correlations\\Heat_Maps\\220180410{}_HMTweets_{}_{}.xls'.format(sentiment_dict, corr_var_stock, corr_var_sent), encoding='uft-8')
        heatmap_days.to_excel('C:\\Users\\jonas\\Documents\\BA_JonasIls\\Literatur & Analysen\\Correlations\\Heat_Maps\\220180410{}_HMDays_{}_{}.xls'.format(sentiment_dict, corr_var_stock, corr_var_sent), encoding='uft-8')

    else: 'Do Nothing'
    return(heatmap_corr)


if __name__ == "__main__":

    LM = 'SentimentLM'
    GI = 'SentimentGI'
    HE = 'SentimentHE'
    TB = 'SentimentTB'

    list_of_dicts = [GI, LM, HE]

    # Define companies you'd like to analyze
    companies = ['$AAPL', '$MSFT', '$MMM', '$AXP', '$BA', '$CAT', '$CVX', '$CSCO', '$KO', '$DWDP', '$DIS', '$XOM', '$GE','$GS', '$HD', '$IBM', '$INTC', '$JNJ', '$JPM', '$MCD', '$MRK', '$NKE', '$PFE', '$PG', '$TRV', '$UTX', '$UNH', '$VZ', '$V', '$WMT']
    #companies = ['BA']
    companies = [company.replace('$', '') for company in companies]


    corr_var_stock = 'abnormal_returns'
    corr_var_sent = 'sent_mean_w'

    filter = [0, 25, 50, 75]

    file_path = 'C:\\Users\\jonas\\Documents\\BA_JonasIls\\Twitter_Streaming\\Sentiment_Dataframes\\20180101_20180410\\20180101_20180410_SentimentDataframes_{}'

    main_correlation_stockwise(list_of_companies= companies,
                               list_of_dicts = [HE],
                               list_of_corr_var_sent = ['bullishness_a', 'bullishness_d', 'bullishness_b', 'bullishness', 'bullishness_w_d',
                                                        'sent_mean_w_a', 'sent_mean_w_d', 'sent_mean_w_b', 'sent_mean_w'],
                               corr_var_stock='abnormal_returns',
                               percentile_tweetcount=50,
                               sent_min=0,
                               sentiment_filter=True,
                               volume_filter=True)

    #main_correlation_allstocks(HE, 5 0, 0, 'spearman')

#    l = list()
#    for company in companies:
#        df_weekly = main_correlation_weekly(company, HE)
#        l.append(df_weekly)

#    df_weekly = pd.concat(l)

#    file_path = 'C:\\Users\\jonas\\Documents\\BA_JonasIls\\Literatur & Analysen\\Correlations\\20180410_CorrAllStocks{}_weekly.xls'.format(HE)
#    df_weekly = df_weekly.corr()
#    df_weekly.to_excel(file_path, encoding='utf-8')

