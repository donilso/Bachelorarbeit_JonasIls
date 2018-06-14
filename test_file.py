import pandas as pd

def show_stream(file_path):
    df = pd.read_csv(file_path, encoding='utf-8')
    df['date'] = pd.to_datetime(df['date'])
    df['time_adj'] = pd.to_datetime(df.time_adj)
    df = df[['date', 'time_adj']]
    df['hour'] = df['time_adj'].dt.hour
    df = df.groupby(['date', 'hour']).count()

    df.to_excel('C:\\Users\\jonas\\Documents\\twitterstream_MarchApril.xls')

    return df

def merge_df_sent(companies, filepath_1, filepath_2):
    for company in companies:

        df_1 = pd.read_csv(file_path_1.format(company), encoding="utf-8")
        print(len(df_1))
        df_2 = pd.read_csv(file_path_2.format(company), encoding="utf-8")
        print(len(df_2))
        df = pd.concat([df_1, df_2])
        print(len(df))

        df.to_csv(
            'C:\\Users\\jonas\\Documents\\BA_JonasIls\\Twitter_Streaming\\Sentiment_Dataframes\\20180101_20180410\\20180101_20180410_SentimentDataframes_{}'.format(
                company))

        return df

companies = ['$AAPL', '$MSFT', '$MMM', '$AXP', '$BA', '$CAT', '$CVX', '$CSCO', '$KO', '$DWDP', '$DIS', '$XOM', '$GE','$GS', '$HD', '$IBM', '$INTC', '$JNJ', '$JPM', '$MCD', '$MRK', '$NKE', '$PFE', '$PG', '$TRV', '$UTX', '$UNH', '$VZ', '$V', '$WMT']
companies = [company.replace('$', '') for company in companies]

file_path_1 = 'C:\\Users\\jonas\\Documents\\BA_JonasIls\\Twitter_Streaming\\Sentiment_Dataframes\\spam_cleaned\\20180101_20180217_SentimentDataframes_{}'
file_path_2 = 'C:\\Users\\jonas\\Documents\\BA_JonasIls\\Twitter_Streaming\\Sentiment_Dataframes\\20180613\\20180305_20180613_SentimentDataframes_{}'
file_path = 'C:\\Users\\jonas\\Documents\\BA_JonasIls\\Twitter_Streaming\\Sentiment_Dataframes\\20180101_20180410\\20180101_20180410_SentimentDataframes_AAPL'

print(show_stream(file_path))

