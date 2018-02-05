
import pandas_datareader.data as web
import datetime

print("Collecting Stock Data")

start = datetime.datetime(2018, 1, 2)
end = datetime.datetime(2018, 1, 7)

stockdata = web.DataReader("MSFT", 'yahoo', start, end)

file = open ('C:\\Users\\Open Account\\Documents\\BA_JonasIls\\Twitter_Streaming\\Test_StockPrices_MSFT.csv', 'w')
file.close()

sdfile = stockdata.to_csv('Test_stockdata_MSFT.csv')

print("Finished")