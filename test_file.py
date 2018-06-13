import pandas as pd



file_path = 'C:\\Users\\jonas\\Documents\\BA_JonasIls\\Twitter_Streaming\\Feeds\\20180613\\20180305_20180613_twitterstreaming_AAPL.csv'


def show_stream(filepath):
    df = pd.read_csv(file_path, encoding='utf-8', delimiter='#')
    df['date'] = pd.to_datetime(df['date'])
    df['time_adj'] = pd.to_datetime(df.time_adj)
    df = df[['date', 'time_adj']]
    df['hour'] = df['time_adj'].dt.hour
    df = df.groupby(['date', 'hour']).count()

    df.to_excel('C:\\Users\\jonas\\Documents\\twitterstream_MarchApril.xls')


companies = ['$AAPL', '$MSFT', '$MMM', '$AXP', '$BA', '$CAT', '$CVX', '$CSCO', '$KO', '$DWDP', '$DIS', '$XOM', '$GE','$GS', '$HD', '$IBM', '$INTC', '$JNJ', '$JPM', '$MCD', '$MRK', '$NKE', '$PFE', '$PG', '$TRV', '$UTX', '$UNH', '$VZ', '$V', '$WMT']
companies = [company.replace('$', '') for company in companies]

for company in companies:
    df = pd.read_csv('C:\\Users\\jonas\\Documents\\twitterstreams_test\\20180305_20180613_twitterstreaming_{}.csv'.format(company), sep='#', encoding='utf-8')
    print(company)
    print(df.date.unique())
    print(len(df))

    file_path_1 = 'C:\\Users\\jonas\\Documents\\BA_JonasIls\\Twitter_Streaming\\Sentiment_Dataframes\\spam_cleaned\\20180101_20180217_SentimentDataframes_{}'.format(company)
    file_path_2 = 'C:\\Users\\jonas\\Documents\\BA_JonasIls\\Twitter_Streaming\\Sentiment_Dataframes\\20180613\\20180305_20180613_SentimentDataframes_{}'.format(company)
    df_1 = pd.read_csv(file_path_1, encoding="utf-8")
    print(len(df_1))
    df_2 = pd.read_csv(file_path_2, encoding="utf-8")
    print(len(df_2))
    df = pd.concat([df_1, df_2])
    print(len(df))
    df.to_csv('C:\\Users\\jonas\\Documents\\BA_JonasIls\\Twitter_Streaming\\Sentiment_Dataframes\\20180101_20180410\\20180101_20180410_SentimentDataframes_{}'.format(company))