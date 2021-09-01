# -*- coding: utf-8 -*-
"""
Created on Wed Sep  1 10:11:15 2021

@author: Anar
"""
import urllib.request
import json
import pandas as pd
import time
import logging
from sqlalchemy import create_engine
from sqlalchemy import exc
from datetime import datetime
import traceback 

class TradesFetcher:
    def __init__(self, url, username, password, symbol, 
                 schema, database = 'Ticks', host = 'localhost:5432',
                 vol_curr = 'COIN', price_curr = 'USD'):
        self.last_chunk = []
        self.url = url
        self.vol_curr = vol_curr
        self.price_curr = price_curr
        self.symbol = symbol
        self.schema = schema
        self.engine = create_engine('postgresql://'+username+':'+password+'@'+host+'/'+database)
        
    def request(self): # пользователем вызывается этот метод
        user_agent = 'Mozilla/5.0 (platform; rv:geckoversion) Gecko/geckotrail Firefox/firefoxversion'
        request = urllib.request.Request(self.url)
        request.add_header('User-Agent', user_agent)
        try: #сделать нормальную обработку исключений
            result = urllib.request.urlopen(request, timeout = 5)
        except urllib.error.HTTPError as e:
            assert False, '{rs}, code: {cd}'.format(cd = e.code, rs = e.reason)
        self.check_response(result)  

    def check_response(self, response):
        try:
            J = json.loads(response.read())
        except:
            assert False, 'A problem with json parsing has occured, response:{resp}'.format(resp = response.read())
        assert  len(J) != 0, 'Response is empty'
        data_chunk = pd.DataFrame(J)
        self.parse_response(data_chunk)
         
    def parse_response(self, data_chunk): # этот метод переопределяем, остальные можно не трогать
        pass
        
    def write(self, data_chunk):
        if len(self.last_chunk) == 0:
            self.last_chunk = data_chunk
            for i in range(len(data_chunk)):
                try:
                    data_chunk.iloc[i:i+1].to_sql(self.symbol, self.engine, if_exists = 'append', index = False, schema = self.schema)
                except exc.IntegrityError:
                    pass
        else:
            conc = pd.concat([data_chunk, self.last_chunk, self.last_chunk])
            conc.drop_duplicates(subset = 'trade_id', keep=False, inplace = True)
            self.last_chunk = data_chunk
            if len(conc) == 0:
                return
            conc.to_sql(self.symbol, self.engine, if_exists = 'append', index = False, schema = self.schema)
            print(datetime.now(),'query is committed, {l} rows are added'.format(l=len(conc))) # обязательно добавить в лог
            
class CEX_TradesFetcher(TradesFetcher):    
    def parse_response(self, data_chunk):
        data_chunk['vol_curr'] = [self.vol_curr for i in range(len(data_chunk))]
        data_chunk['price_curr'] = [self.price_curr for i in range(len(data_chunk))]
        data_chunk['symbol'] = [self.symbol for i in range(len(data_chunk))]
        #data_chunk['exchange'] = ['CEX' for i in range(len(data_chunk))]
        data_chunk['localtime'] = [datetime.now() for i in range(len(data_chunk))]
        data_chunk.columns = ['side', 'timestamp', 'volume', 'price', 'trade_id', 'vol_curr', 'price_curr',
       'symbol', 'localtime']
        data_chunk['timestamp'] = pd.to_datetime(data_chunk['timestamp'],unit = 's')
        data_chunk['price'] = data_chunk['price'].apply(float)
        data_chunk.replace('buy','b',inplace = True)
        data_chunk.replace('sell','s', inplace = True)
        data_chunk['volume'] = data_chunk['volume'].apply(float)
        self.write(data_chunk)
    
class OKEX_TradesFetcher(TradesFetcher):    
    def parse_response(self, data_chunk):
        data_chunk.drop('time', inplace = True, axis = 1)
        data_chunk['timestamp'] = pd.to_datetime(data_chunk['timestamp'])
        data_chunk['side'].replace('sell','s',inplace = True)
        data_chunk['side'].replace('buy','b', inplace = True)
        data_chunk['size'] = data_chunk['size'].apply(float)
        data_chunk['price'] = data_chunk['price'].apply(float)
       # data_chunk['exchange'] = ['CEX' for i in range(len(data_chunk))]
        data_chunk['symbol'] = [self.symbol for i in range(len(data_chunk))]
        data_chunk['vol_curr'] = [self.vol_curr for i in range(len(data_chunk))]
        data_chunk['price_curr'] = [self.price_curr for i in range(len(data_chunk))]
        data_chunk.rename({'size':'volume'},inplace = True, axis = 1)
        self.write(data_chunk)
        
class COINBENE_TradesFetcher(TradesFetcher):  
    pass

class DERIBIT_TradesFetcher(TradesFetcher):  
    pass
#вытягиваем окекс
'''
url = 'https://www.okex.com/api/spot/v3/instruments/ETH-USDT/trades?limit=100'
okex_obj = OKEX_TradesFetcher(url, 'postgres','admin','ETHUSDT', price_curr = 'USDT',
                              schema = "Okex")
okex_obj.request()
symbols = [('ADA','USDT'),('XRP','USDT'),
           ('BTC','USDT'),('ETH','USDT'),
           ('XLM','USDT'),('BCH', 'USDT'),
           ('BAT', 'USDT')]
urls = ['https://www.okex.com/api/spot/v3/instruments/'+i[0]+'-'+i[1]+'/trades?limit=100' for i in symbols]
objs = [OKEX_TradesFetcher(urls[i], 'postgres','admin',o[0]+o[1], price_curr = o[1], schema = 'Okex') for i,o in enumerate(symbols)]
print([(urls[i], o[0]+o[1], o[1]) for i,o in enumerate(symbols)])
try:
    while True:
            for i,o in enumerate(objs):
                try:
                    print(symbols[i])
                    o.request()
                except Exception as e:
                    print(datetime.now(),type(e), e.args)
            time.sleep(2)

except KeyboardInterrupt:
            print('Execution stopped by the user')
          #  break
'''
#вытягиваем ceх
'''
symbols = [('ADA','USDT'),('XRP','USDT'),
           ('BTC','USDT'),('ETH','USDT'),
           ('XLM','USDT'),('BCH', 'USDT'),
           ('BAT', 'USDT'),
           ('ADA','USD'),('XRP','USD'),
           ('BTC','USD'),('ETH','USD'),
           ('XLM','USD'),('BCH', 'USD'),
           ('BAT', 'USD')]
urls = ['https://cex.io/api/trade_history/'+i[0]+'/'+i[1]+'/' for i in symbols]
objs = [CEX_TradesFetcher(urls[i], 'postgres','admin',o[0]+o[1], schema = 'CEX', price_curr = o[1]) for i,o in enumerate(symbols)]
print([(urls[i], o[0]+o[1], o[1]) for i,o in enumerate(symbols)])
try:
    while True:
            for i,o in enumerate(objs):
                try:
                    print(symbols[i])
                    o.request()
                except Exception as e:
                    print(datetime.now(),type(e), e.args)
            time.sleep(30)

except KeyboardInterrupt:
            print('Execution stopped by user')
          #  break
      '''