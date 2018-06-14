
import pandas_datareader.data as web
import datetime

print("Collecting Stock Data")

start = datetime.datetime(2017, 1, 1)
end = datetime.datetime(2018, 6, 1)

companies = ['$MSFT', '$MMM', '$AXP', '$AAPL', '$BA', '$CAT', '$CVX', '$CSCO', '$KO', '$DWDP', '$DIS', '$XOM', '$GE',
             '$GS', '$HD', '$IBM', '$INTC', '$JNJ', '$JPM', '$MCD', '$MRK', '$NKE', '$PFE', '$PG', '$TRV', '$UTX',
            '$UNH', '$VZ', '$V', '$WMT', 'DIA']

companies = [company.replace('$', '') for company in companies]

for company in companies:

    try:
        stockdata = web.DataReader(company, 'iex', start, end)
        stockdata = stockdata.reset_index()
        stockdata.columns = ['Date', 'Open', 'High', 'Low', 'Close',  'Volume']
        print(stockdata)

        stockdata.to_csv('C:\\Users\\jonas\\Documents\\BA_JonasIls\\Stock_Quotes\\20180613StockPrices_{}.csv'.format(company), encoding='utf-8')

    except Exception as e:
        print(company, ":", e)


    #sdfile = stockdata.to_csv('Test_stockdata_MSFT.csv')

print("Finished")