import pandas as pd
import numpy as np
import datetime
import time
import statsmodels.api as sm
from statsmodels.iolib.summary2 import summary_col

import linearmodels
from linearmodels import PanelOLS

import seaborn as sns
import matplotlib as mpl
import matplotlib.pyplot as plt


def open_df_c2c(company, sentiment_dict, vol_min, sent_min, data):
    if data == 'news':
        file_path = 'C:\\Users\\jonas\\Documents\\BA_JonasIls\\Newsfeeds\\Sentiment_Dataframes\\C2C_Dataframes\\NewsC2C_{}_{}_{}vol'.format(company, sentiment_dict, vol_min)
        df = pd.read_csv(file_path, encoding="utf-8", parse_dates=True, index_col=0)
        df.index.names = ['date']

        return df

    elif data == 'twitter':
        file_path = 'C:\\Users\\jonas\\Documents\\BA_JonasIls\\Twitter_Streaming\\Sentiment_Dataframes\\20180101_20180410\\C2C_Dataframes\\c2c_20180101_20180410{}_{}_Vol{}_Sent{}'.format(company, sentiment_dict, vol_min, sent_min)
        df = pd.read_csv(file_path, encoding="utf-8", parse_dates=True, index_col=0)
        df.index = df.index.astype(str)
        #print(len(df))

        return df

def regression_analysis(X, Y, data):
    # get data and manipulate data frame
    df = open_df_c2c('AllStocks', 'SentimentHE', 0, 0, data=data)
    df = df.dropna(subset= X + [Y])
    df['abnormal_returns'] = df['abnormal_returns']*100
    df['tweet_count'] = np.log(df['tweet_count'])
    df['volume_dollar'] = np.log(df['volume_dollar'])

    # define model variables and add an intercept
    Y = df[Y]
    X = df[X]
    X = sm.add_constant(X)

    model = sm.OLS(Y, X).fit(cov_type='HAC', cov_kwds={'maxlags':1})
    predictions = model.predict(X)
    return model.summary()

def panel_regression(X, Y, sent_dict, data, cov_type):
    companies = ['$AAPL', '$MSFT', '$MMM', '$AXP', '$BA', '$CAT', '$CVX', '$CSCO', '$KO', '$DWDP', '$DIS', '$XOM',
                 '$GE', '$GS', '$HD', '$IBM', '$INTC', '$JNJ', '$JPM', '$MCD', '$MRK', '$NKE', '$PFE', '$PG', '$TRV',
                 '$UTX', '$UNH', '$VZ', '$V', '$WMT']
    companies = [company.replace('$', '') for company in companies]

    l = list()
    for company in companies:
        df = open_df_c2c(company, sent_dict, 0, 0, data=data)
        df['company'] = company
        l.append(df)

    # create overall c2c-dataframe with adaquate index for panel regression
    df = pd.concat(l)
    df = df.reset_index()
    df.date = pd.to_datetime(df.date)
    df = df.set_index(['company', 'date'])

    # clean dataframe from NAs and modify metrics to get interpretable results
    df = df.dropna(subset= X + [Y])
    df['abnormal_returns'] = df['abnormal_returns']*100
    # define model variables and add an intercept

    if data == "twitter":
        df['tweet_count'] = np.log(df['tweet_count'])

    if data == "news":
        df['news_count'] = np.log(df['news_count'])

    df['volume_dollar'] = np.log(df['volume_dollar'])
    df['Volume'] = np.log(df['Volume'])

    Y = df[Y]
    X = df[X]
    X = sm.add_constant(X)

    # estimate linear model with fixed effects for panel data
    model = linearmodels.PanelOLS(Y, X, entity_effects=True)

    if cov_type == None:
        results = model.fit(cov_type='clustered', cluster_entity=True)
    else:
        results = model.fit(cov_type=cov_type)

    print(results)

    return results

# estimate three linear models for all three stock features
X = ['daily_returns_index', 'bullishness', 'news_count', 'sent_std']
#X = ['bullishness_d', 'bullishness_a', 'bullishness_b']

Y1 = 'abnormal_returns'
Y2 = 'volume_dollar'
Y3 = 'volatility_parks'

model1 = panel_regression(X, Y1, 'SentimentHE', 'news', 'robust')
model2 = panel_regression(X, Y2, 'SentimentHE', 'news', 'robust')
model3 = panel_regression(X, Y3, 'SentimentHE', 'news', 'robust')

# generate output for all three models
dfoutput = summary_col([model1, model2, model3], stars=True)
print(dfoutput)

# safe output
today = str(datetime.date.today().strftime("%Y%m%d"))
file_path = "C:\\Users\\jonas\\Documents\\BA_JonasIls\\Literatur & Analysen\\Regression\\{}_panel_regression".format(today)
dfoutput.as_latex(file_path)

sentiment_dict = 'SentimentHE'
company = 'AAPL'
vol_min = 0
sent_min = 0

df_twi = open_df_c2c('AAPL', 'SentimentHE', 0, 0, 'twitter')
df_news = open_df_c2c('AAPL', 'SentimentHE', 0, 0, 'news')

df_twi = df_twi[['abnormal_returns', 'daily_returns_index']]
df_news = df_news[['abnormal_returns', 'daily_returns_index']]

df_twi.head(10)

print('News')
df_news.head(10)