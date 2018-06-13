import pandas as pd



file_path = 'C:\\Users\\jonas\\Documents\\BA_JonasIls\\Twitter_Streaming\\Feeds\\20180613\\20180305_20180613_twitterstreaming_MSFT.csv'



df = pd.read_csv(file_path, encoding='utf-8')

df = df[['date', 'time_adj']]
df['hour'] = df['time_adj'].dt.hour

df = df.groupby(['date', 'hour'])

print(df)

