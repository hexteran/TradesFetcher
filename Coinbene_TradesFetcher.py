# -*- coding: utf-8 -*-
"""
Created on Thu Sep  2 13:24:50 2021

@author: Anar
"""

import pandas as pd
import time
import logging
from sqlalchemy import create_engine
from sqlalchemy import exc
from datetime import datetime
from TradesFetcher import TradesFetcher
import urllib
import json

logname = 'Coinbene_TradesFetcher'
symbols = [('ADA','USDT'),('XRP','USDT'),
           ('BTC','USDT'),('ETH','USDT'),
           ('XLM','USDT'),('BCH', 'USDT'),
           ('LTC', 'USDT')]

logging.basicConfig(filename=logname+'.log', filemode='a',
                            format='%(asctime)s.%(msecs)d,%(levelname)s:%(message)s',
                            datefmt='%Y-%m-%d %H:%M:%S', level = logging.DEBUG)

logger = logging.getLogger("err")
formatter = logging.Formatter('%(asctime)s.%(msecs)d,%(levelname)s:%(message)s',
                              datefmt='%Y-%m-%d %H:%M:%S')
file_handler = logging.FileHandler(filename=logname+"_error.log")
file_handler.setFormatter(formatter)
file_handler.setLevel(logging.ERROR)
logger.addHandler(file_handler)

class COINBENE_TradesFetcher(TradesFetcher):  
    def check_response(self, response):
        try:
            J = json.loads(response.read())
            J = J['data']
        except:
            assert False, 'A problem with json parsing has occured, response:{resp}'.format(resp = response.read())
        assert  len(J) != 0, 'Response is empty'
        data_chunk = pd.DataFrame(J)
        self.parse_response(data_chunk)
        
    def parse_response(self, data_chunk):
        data_chunk.rename({0:'symbol', 1:'price',2:'volume',3:'side',4:'timestamp'},axis = 1, inplace = True)
        data_chunk['symbol'] = [self.symbol for i in range(len(data_chunk))]
        data_chunk['price_curr'] = [self.price_curr for i in range(len(data_chunk))]
        data_chunk['vol_curr'] = [self.vol_curr for i in range(len(data_chunk))]
        data_chunk.replace({'buy':'b','sell':'s'}, inplace = True)
        data_chunk['timestamp'] = pd.to_datetime(data_chunk['timestamp'])
        data_chunk['price'] = data_chunk['price'].apply(float)
        data_chunk['volume'] = data_chunk['volume'].apply(float)
        data_chunk['localtime'] = [datetime.now() for i in range(len(data_chunk))]
        #data_chunk[['price','volume']].astype(float)
        self.write(data_chunk)
        #self.parse_response(data_chunk)        
        
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
            conc.drop_duplicates(subset = ['symbol','price','volume','side','timestamp'], keep=False, inplace = True)
            self.last_chunk = data_chunk
            if len(conc) == 0:
                return
            conc.to_sql(self.symbol, self.engine, if_exists = 'append', index = False, schema = self.schema)
            print(datetime.now(),'query is committed, {l} rows are added'.format(l=len(conc))) # обязательно добавить в лог

urls = ['https://openapi-exchange.coinbene.com/api/exchange/v2/market/trades?symbol='+i[0]+i[1] for i in symbols]
objs = [COINBENE_TradesFetcher(urls[i], 'postgres','admin',o[0]+o[1], schema = 'Coinbene', price_curr = o[1]) for i,o in enumerate(symbols)]
try:
    while True:
            for i,o in enumerate(objs):
                try:
                    print(symbols[i])
                    o.request()
                except urllib.error.HTTPError as e:
                    message = str(symbols[i])+ 'HTTP error occured, code: '+str(e.code)+' '+'on request:'+urls[i]
                    print(message)
                    logger.error(message)
                except Exception as e:
                    message = str(symbols[i])+', type:'+str(type(e))+', args:'+str(e.args)
                    print(message)
                    logger.error(message)
            time.sleep(60)

except KeyboardInterrupt:
            print('Execution stopped by the user')
            logging.debug('Execution stopped by the user')