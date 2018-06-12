import pandas as pd
import numpy as np
import datetime
import statsmodels.formula.api as sm

import seaborn as sns
import matplotlib as mpl
import matplotlib.pyplot as plt

from sklearn import linear_model
from sklearn.cross_validation import train_test_split
from sklearn.metrics import mean_squared_error, r2_score


def open_df_c2c(company, sentiment_dict, vol_min, sent_min, data):
    if data == 'news':
        file_path = 'C:\\Users\\Open Account\\Documents\\BA_JonasIls\\Newsfeeds\\Sentiment_Dataframes\\C2C_Dataframes\\NewsC2C_{}_{}_{}vol'.format(company, sentiment_dict, vol_min)
        df = pd.read_csv(file_path, encoding="utf-8", parse_dates=True)
        return df

    elif data == 'twitter':
        bad_days = ['2018-01-04', '2018-01-03', '2018-01-02', '2018-01-01', '2018-01-25', '2018-01-26', '2018-01-27',
                    '2018-02-02', '2018-02-03', '2018-02-06', '2018-02-10']
        file_path = 'C:\\Users\\Open Account\\Documents\\BA_JonasIls\\Twitter_Streaming\\Sentiment_Dataframes\\20180101_20180217\\C2C_Dataframes\\20180101_20180217_C2C{}_{}_{}vol_{}sen.csv'.format(
            company, sentiment_dict, vol_min, sent_min)
        df = pd.read_csv(file_path, encoding="utf-8", index_col=0)
        df.index = df.index.astype(str)
        print(len(df))

        for bad_day in bad_days:
            try:
                df = df.drop(bad_day)
            except Exception as e:
                print(e)

        return df


def regression_onefactor(stock_var, twi_var):
    df = open_df_c2c('AllStocks', 'SentimentGI', 0, 0, data='twitter')
    df.index = df.index.astype(str)

    var_std = ['tweet_count', 'count_pos', 'count_neg', 'tweet_count_w', 'count_pos_w', 'count_neg_w']
    for x in var_std:
        mean = df['{}'.format(x)].mean()
        std = df['{}'.format(x)].std()
        df['{}_std'.format(x)] = ((df['{}'.format(x)] - mean) / std)

    y = df['{}'.format(stock_var)]
    x = df['{}'.format(twi_var)]
    #x = sm.add_constant(x)

    results = sm.OLS(endog=y, exog=x, missing='drop').fit()
    results.save('C:\\Users\\Open Account\\Documents\\BA_JonasIls\\Literatur & Analysen\\Regression\\Regr_{}_{}.pickle'.format(stock_var, twi_var))
    return results

def regression_twofactor(df_c2c, stock_var, twi_var1, twi_var2):
    bad_days = ['2018-01-04', '2018-01-03', '2018-01-02', '2018-01-01', '2018-01-25', '2018-01-26', '2018-01-27', '2018-02-02', '2018-02-03', '2018-02-10']
    df_c2c.index = df_c2c.index.astype(str)
    df_c2c = df_c2c.drop(bad_days)

    results = sm.OLS(formula="{} ~ {} + {}".format(stock_var, twi_var1, twi_var2), data=df_c2c ).fit()
    #results.save('C:\\Users\\Open Account\\Documents\\BA_JonasIls\\Literatur & Analysen\\Regression\\Regr_{}_{}&{}.pickle'.format(stock_var, twi_var1, twi_var2))
    return results



def regression_sklearn():
    # Load the diabetes dataset
    df = open_df_c2c('AllStocks', 'SentimentGI', 0, 0, data='twitter')
    df = df.dropna()

    var_std = ['tweet_count', 'count_pos', 'count_neg', 'tweet_count_w', 'count_pos_w', 'count_neg_w']
    for x in var_std:
        mean = df['{}'.format(x)].mean()
        std = df['{}'.format(x)].std()
        df['{}_std'.format(x)] = ((df['{}'.format(x)] - mean) / std)

    # Use only one feature
    df_x = df[['tweet_count_w_std']]
    df_y = df['volatility_parks']

    # Split the data and target into training/testing sets
    x_train, x_test, y_train, y_test = train_test_split(df_x, df_y, test_size=0.2, random_state=4)

    # Create linear regression object
    regr = linear_model.LinearRegression()

    # Train the model using the training sets
    regr.fit(x_train, y_train)

    # Make predictions using the testing set
    y_pred = regr.predict(x_test)

    # The coefficients
    print('Coefficients: \n', regr.coef_)
    # The mean squared error
    print("Mean squared error: %.2f"
          % mean_squared_error(y_test, y_pred))
    # R Squared
    print('Variance score: %.2f' % r2_score(y_test, y_pred))

    # Plot outputs
    plt.scatter(x_test, y_test,  color='black')

    plt.xticks(())
    plt.yticks(())

    plt.show()


reg = regression_onefactor('volatility_parks', 'tweet_count_w_std')
print(reg.summary())