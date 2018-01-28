
import pandas_datareader.data as web
import datetime

print("Collecting Stock Data")

start = datetime.datetime(2018, 1, 8)
end = datetime.datetime(2018, 1, 9)

stockdata = web.DataReader("F", 'google', start, end)

file = open (r'/Users/Jonas/Desktop/BA_Results/stockdata_google.csv', 'w')
file.close()

sdfile = stockdata.to_csv('stockdata_google.csv')

print("Finished")