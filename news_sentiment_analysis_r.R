library(SentimentAnalysis)


### Utility function to Read dataframe for certain company
read_dataframe_articles = function(company){
  dir_read = 'C:\\Users\\jonas\\Documents\\BA_JonasIls\\Newsfeeds\\Feeds\\'
  file_read = 'Cleaned_Newsfeed_$'
  format = '.csv'
  
  path_read = paste(dir_read, file_read, company, format, sep="")
  
  df_articles = read.csv(path_read, header = TRUE, encoding="utf-8", sep=',')
  #df_articles =  df_articles[ , c("date", "article_clean" ,"time_adj", "Timeslot")]
  
  return(df_articles)
}



### Utility function to write dataframe for certain company
write_dataframe_articles = function(company, dataframe){
  dir_write = 'C:\\Users\\jonast\\Documents\\BA_JonasIls\\Newsfeeds\\Sentiment_Dataframes\\'
  file_write = 'NewsSentimentDataframes_'
  format = '.csv'
  
  path_write = paste(dir_write, file_write, company, format, sep="")
  write.csv(dataframe, path_write, fileEncoding="utf-8", sep=',')
}


### Main Function to create sentiments, add them to dataframe and write them to csv 
sentiment_analysis_articles = function(company){
  df_news = read_dataframe_articles(company = company)
  
  articles = as.vector(df_news['article_clean'])
  
  sentiments=try(SentimentAnalysis::analyzeSentiment(articles), silent=F, outFile = "error analyzing text")
  
  df_news$SentimentLM = sentiments$SentimentLM
  df_news$SentimentGI = sentiments$SentimentGI
  df_news$SentimentHE = sentiments$SentimentHE
  
  write_dataframe_articles(company = company, df_news)
  return(df_news) 
}

companies = c('MSFT', 'MMM', 'AXP', 'AAPL', 'BA', 'CAT', 'CVX', 'CSCO', 'KO', 'DWDP', 'DIS', 'XOM', 'GE', 'GS', 'HD', 'IBM', 'INTC', 'JNJ', 'JPM', 'MCD', 'MRK', 'NKE', 'PFE', 'PG', 'TRV', 'UTX', 'UNH', 'VZ', 'V', 'WMT')

for (company in companies){
  print(company)
  df_articles = sentiment_analysis_articles(company=company)
}

