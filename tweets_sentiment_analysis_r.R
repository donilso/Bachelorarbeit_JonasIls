library(SentimentAnalysis)

### Utility function to Read dataframe for certain company
read_dataframe = function(company){
  dir_read = 'C:\\Users\\jonas\\Documents\\BA_JonasIls\\Twitter_Streaming\\Feeds\\20180613\\'
  file_read = '20180305_20180613_twitterstreaming_'
  format = '.csv'
  
  path_read = paste(dir_read, file_read, company, format, sep="")
  
  df_tweets = read.csv(path_read, header = TRUE, encoding="utf-8", sep="#", dec=".", comment.char = "")
  
  return(df_tweets)
}



### Utility function to write dataframe for certain company
write_dataframe = function(company, dataframe){
  dir_write = 'C:\\Users\\jonas\\Documents\\BA_JonasIls\\Twitter_Streaming\\Sentiment_Dataframes\\20180613\\'
  file_write = '20180305_20180613_SentimentDataframes_'
  format = '.csv'
  
  path_write = paste(dir_write, file_write, company, sep="")
  write.csv(dataframe, path_write, fileEncoding='utf-8')
}



### Main Function to create sentiments, add them to dataframe and write them to csv 
sentiment_analysis = function(company){
  df_tweets = try(read_dataframe(company = company), silent=F, outFile = "error reading file")
  
  text_clean = as.vector(df_tweets['text_clean'])
  
  sentiments=SentimentAnalysis::analyzeSentiment(text_clean)
  
  df_tweets['SentimentLM'] = sentiments['SentimentLM']
  df_tweets['SentimentGI'] = sentiments['SentimentGI']
  df_tweets['SentimentHE'] = sentiments['SentimentHE']
  
  write_dataframe(company = company, df_tweets)
  return(df_tweets) 
}

#'AAPL', 'DIS'
companies = c('AAPL', 'MSFT', 'MMM', 'AXP', 'BA', 'CAT', 'CVX', 'CSCO', 'KO', 'DWDP', 'DIS', 'XOM', 'GE', 'GS', 'HD', 'IBM', 'INTC', 'JNJ', 'JPM', 'MCD', 'MRK', 'NKE', 'PFE', 'PG', 'TRV', 'UTX', 'UNH', 'VZ', 'V', 'WMT')

for (company in companies){
  print(company)
  sentiment_analysis(company)
}