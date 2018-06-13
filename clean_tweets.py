#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Feb 26 20:15:05 2018

@author: thomas
"""

import pandas as pd
from io import StringIO
#df = pd.read_csv('/home/thomas/Desktop/00024012018twitterfeed_AXP.csv', sep='#')
companies = ['$MSFT', '$MMM', '$AXP', '$AAPL', '$BA', '$CAT', '$CVX', '$CSCO', '$KO', '$DWDP', '$DIS', '$XOM', '$GE', '$GS', '$HD', '$IBM', '$INTC', '$JNJ', '$JPM', '$MCD', '$MRK', '$NKE', '$PFE', '$PG', '$TRV', '$UTX', '$UNH', '$VZ', '$V', '$WMT']
companies = [company.replace('$', '') for company in companies]

file_path = 'C:\\Users\\jonas\\Documents\\BA_JonasIls\\Twitter_Streaming\\Feeds\\20180613\\20180305_20180613_twitterstreaming_{}.csv'
#file_path = 'C:\\Users\\jonas\\Documents\\twitterstreams_test\\20180305_20180613_twitterstreaming_{}.csv'

count_lines = 0
list = []
for company in companies:
    all_text = ""
    counter = 0
    bad_lines = 0

    with open(file_path.format(company), 'r', encoding='utf-8') as f:
        print(company)

        count_lines = 0

        try:
            for line in f:
                count_lines = count_lines + 1
                print(company, count_lines)

                line = line.replace('\n', '')
                line = line.replace('\r', '')
                #line = line.replace('#date#','#date1#')
                line = line.replace('#symbols','#symbols\r\n')

        #        if counter == 44:
        #            break

                line = line.replace("#['{}']#['{}']".format(company, company),"#['{}']#['{}']\r\n".format(company, company))
                #line = line.replace('#during', '#during\r\n')
                #line = line.replace('#after', '#after\r\n')
                all_text = all_text + str(line)

        except Exception as e:
            bad_lines = bad_lines + 1
            print(e)


            counter = counter + 1
        list.append(bad_lines)


    #to_parse = all_text[0:200]
    all_text_io = StringIO(all_text)
    print(all_text_io)
    df = pd.read_csv(all_text_io, sep='#', parse_dates=True, infer_datetime_format=True, warn_bad_lines=True, error_bad_lines=False)
    df.set_index('date', inplace=True)
    df.drop_duplicates(subset='id')
    f.close()
    df.to_csv('C:\\Users\\jonas\\Documents\\twitterstreams_test\\20180305_20180613_twitterstreaming_{}.csv'.format(company), sep='#')

print(list)
