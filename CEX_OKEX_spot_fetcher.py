import requests
import pandas as pd
import numpy as np
import time
from datetime import datetime

names_first = ['XRP','ADA','LTC','ETH','BTC','XLM','BAT','BCH]
names_second = ['USD', 'USDT']

path_cex = 'C:/Buffer/CEX/'
path_okex = 'C:/Buffer/Okex/'
thr_cex = 90
thr_okex = 5


file_cex = {}
file_okex = {}
 
beg_cex = 0
beg_okex = 0
while True:
    if time.time()-beg_cex>90:       
        for n1 in names_first:
            for n2 in names_second:
                try:
                    url = 'https://cex.io/api/trade_history/'+n1+'/'+n2+'/'
                    response = requests.request("GET", url)
                    file_cex[n1+n2] = open(path_cex+n1+n2+'.txt','a')
                    T = (datetime.now())
                    file_cex[n1+n2].write('{\"localTime\":\"' + str(T) + '\",\"data\":' + response.text+'}\n')
                    file_cex[n1+n2].close()
                    print(T,' CEX: all is fine')
                except:
                    print(T,' CEX: smt is wrong')
        beg_cex = time.time()
                
    if time.time()-beg_okex>5:
        for n1 in names_first:
            for n2 in names_second[1:2]:
                try:
                    url = 'https://www.okex.com/api/spot/v3/instruments/'+n1+'-'+n2+'/trades?limit=100'
                    response = requests.request("GET", url)
                    file_okex[n1+n2] = open(path_okex+n1+n2+'.txt','a')
                    T = (datetime.now())
                    file_okex[n1+n2].write('{\"localTime\":\"' + str(T) + '\",\"data\":' + response.text+'}\n')
                    file_okex[n1+n2].close()
                    print(T,' OKEX: all is fine')
                except:
                    print(T,' OKEX: smt is wrong')
        beg_okex = time.time()         
                
    