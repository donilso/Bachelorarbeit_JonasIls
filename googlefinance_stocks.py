
import pandas_datareader.data as web
import datetime

print("Collecting Stock Data")

start = datetime.datetime(2017, 1, 1)
end = datetime.datetime(2018, 2, 1)

#companies = ['$MSFT', '$MMM', '$AXP', '$AAPL', '$BA', '$CAT', '$CVX', '$CSCO', '$KO', '$DWDP', '$DIS', '$XOM', '$GE',
#             '$GS', '$HD', '$IBM', '$INTC', '$JNJ', '$JPM', '$MCD', '$MRK', '$NKE', '$PFE', '$PG', '$TRV', '$UTX',
#             '$UNH', '$VZ', '$V', '$WMT']

companies = ['^DJI']
companies = [company.replace('$', '') for company in companies]

for company in companies:

    try:
        stockdata = web.DataReader(company, 'yahoo', start, end)
        stockdata.to_csv('C:\\Users\\Open Account\\Documents\\BA_JonasIls\\Stock_Quotes\\20180201StockPrices_{}.csv'.format(company), encoding='utf-8')

    except Exception as e:
        print(company, ":", e)


    #sdfile = stockdata.to_csv('Test_stockdata_MSFT.csv')

print("Finished")