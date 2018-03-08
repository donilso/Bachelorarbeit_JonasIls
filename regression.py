import pandas as pd
import numpy as np
import datetime
import statsmodels.formula.api as sm

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

stock_var = 'Volume'
twi_var1 = 'bullishness'
twi_var2 = 'pol_pos'

df_c2cAllStocks = pd.read_csv('C:\\Users\\Open Account\\Documents\\BA_JonasIls\\Literatur & Analysen\\Correlations\\All_Stocks\\SentimentGI\\20180217_DF_C2CSentimentGI_0vol_0sen.csv', encoding='utf-8', index_col=[0])
results_regression = regression_onefactor(df_c2cAllStocks, stock_var, twi_var1)
#results_regression = regression_twofactor(df_c2cAllStocks, stock_var, twi_var1, twi_var2)
print(results_regression.summary())

