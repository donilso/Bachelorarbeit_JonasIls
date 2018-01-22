import pandas as pd
import itertools


key_ep = pd.read_excel('C:\\Users\\Open Account\\Documents\\n-ergie\\Keywords_EP.xlsx')
key_eg = pd.read_excel('C:\\Users\\Open Account\\Documents\\n-ergie\\Keywords_EG.xlsx')
names = pd.read_excel('C:\\Users\\Open Account\\Documents\\n-ergie\\Namen.xlsx')

list_ep = key_ep['Keywords EP'].tolist()
list_eg = key_eg['Keywords EG'].tolist()
list_names = names['Name'].tolist()

a = []
tuples = []


for name in list_names:

    parsed_name = {}

    parsed_name['name'] = name
    parsed_name['ep'] = [x for x in list_ep if x in parsed_name['name']]
    parsed_name['eg'] = [x for x in list_eg if x in parsed_name['name']]
    parsed_name['comb'] = list(itertools.product(parsed_name['ep'], parsed_name['eg']))

    a.append(parsed_name)

df = pd.DataFrame(a)

print(df)