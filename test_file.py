import pandas as pd
import numpy as np
from decimal import Decimal

#tweets_data_path = 'C:\\Users\\Open Account\\Documents\\BA_JonasIls\\Twitter_Streaming\\Excel\\twittertext_MSFT.xls'
#tweets_file = pd.read_excel(tweets_data_path, index_label='date', encoding="utf-8")

#tweets_file.to_csv('C:\\Users\\Open Account\\Documents\\BA_JonasIls\\Twitter_Streaming\\Excel\\twittertext_MSFT.csv')
#print(tweets_file)

tweets_data_path = 'C:\\Users\\Open Account\\Documents\\BA_JonasIls\\Twitter_Streaming\\ev_dicts.csv'
tweets_file = pd.read_csv(tweets_data_path, encoding="utf-8")

#def ev(df, sent_score, rel_score):

TR = 0
FR = 0
FI = 0
TI = 0
for date, tweet in tweets_file.iterrows():

    sent = Decimal(tweet['SentimentLM'])
    rel = Decimal(tweet['sent_score'])

    if sent < 0:
        sent = sent * (-1)
    else:
        sent = sent

    if sent > 0.2 and rel == 3:
        TR = TR + 1
    elif sent > 0.2 and rel != 3:
        FR = FR + 1
    elif sent < 0.2 and rel == 3:
        FI = FI + 1
    else:
        TI = TI + 1

rel = [TR, FR]
irrel = [FI, TI]

#matrix_ev = pd.concat(objs=[rel, irrel])








##### Funtion

def dict_evaluation(dataframe, dict):
    TR = 0
    FR = 0
    FI = 0
    TI = 0

    nothing = 0

    for date, tweet in dataframe.iterrows():

        sent = Decimal(tweet['{}'.format(dict)])
        rel = Decimal(tweet['sent_score'])

        if sent < 0:
            sent = sent * (-1)
        else:
            sent = sent

        #print('sent={} and rel={}'.format(sent, rel))

        if sent > 0.2 and rel == 3:
            TR = TR + 1
        elif sent > 0.2 and rel != 3:
            FR = FR + 1
        elif sent < 0.2 and rel == 3:
            FI = FI + 1
        elif sent < 0.2 and rel != 3:
            TI = TI + 1

        else:
            nothing = nothing + 1

    print(nothing)
    ev_dict = {}
    ev_dict['sigificant_{}'.format(dict)] = [TR, FR]
    ev_dict['insignificant_{}'.format(dict)] = [FI, TI]

    ev_matrix = pd.DataFrame.from_dict(ev_dict, orient='index')
    ev_matrix['Sum'] = ev_matrix[0]+ev_matrix[1]
    ev_matrix.rename(columns={0:'relevant', 1:'irrelevant'}, inplace=True)
    print(ev_matrix)

if __name__ == "__main__":
    LM = 'SentimentLM'
    GI = 'SentimentGI'
    HE = 'SentimentHE'
    QDAP = 'SentimentQDAP'
    sent_dicts = [LM, GI, HE, QDAP]

    for sent_dict in sent_dicts:
        dict_evaluation(tweets_file, sent_dict)
