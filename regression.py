import pandas as pd
import numpy as np
import datetime
import statsmodels.formula.api as sm

def open_df_c2c(company, sentiment_dict, vol_min):
    file_path = 'C:\\Users\\Open Account\\Documents\\BA_JonasIls\\Newsfeeds\\Sentiment_Dataframes\\C2C_Dataframes\\NewsC2C_{}_{}_{}vol'.format(company, sentiment_dict, vol_min)
    return pd.read_csv(file_path, encoding="utf-8", parse_dates=True)


def regression_onefactor(df_c2c, stock_var, twi_var):
    bad_days = ['2018-01-04', '2018-01-03', '2018-01-02', '2018-01-01', '2018-01-25', '2018-01-26', '2018-01-27', '2018-02-02', '2018-02-03', '2018-02-10']
    df_c2c.index = df_c2c.index.astype(str)
    df_c2c = df_c2c.drop(bad_days)

    y = df_c2c['{}'.format(stock_var)]
    x = df_c2c['{}'.format(twi_var)]
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


import matplotlib.pyplot as plt
import numpy as np
from sklearn import linear_model
from sklearn.cross_validation import train_test_split
from sklearn.metrics import mean_squared_error, r2_score

# Load the diabetes dataset
df = open_df_c2c('AllStocks', 'SentimentGI', 0)
df = df.dropna()


# Use only one feature
df_x = df[['news_count', 'pol_pos', 'pol_neg', 'ratio_pos', 'ratio_neg']]
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
plt.plot(x_test, y_pred, color='blue', linewidth=3)

plt.xticks(())
plt.yticks(())

plt.show()



#stock_var = 'Volume'
#twi_var1 = 'bullishness'
#twi_var2 = 'pol_pos'

#df_c2cAllStocks = pd.read_csv('C:\\Users\\Open Account\\Documents\\BA_JonasIls\\Literatur & Analysen\\Correlations\\All_Stocks\\SentimentGI\\20180217_DF_C2CSentimentGI_0vol_0sen.csv', encoding='utf-8', index_col=[0])
#results_regression = regression_onefactor(df_c2cAllStocks, stock_var, twi_var1)
#results_regression = regression_twofactor(df_c2cAllStocks, stock_var, twi_var1, twi_var2)
#print(results_regression.summary())

