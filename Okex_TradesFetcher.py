# -*- coding: utf-8 -*-
"""
Created on Thu Sep  2 11:44:41 2021

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
 
logname = 'Okex_TradesFetcher'
symbols = [('ADA','USDT'),('XRP','USDT'),
           ('BTC','USDT'),('ETH','USDT'),
           ('XLM','USDT'),('BCH', 'USDT'),
           ('BAT', 'USDT'), ('LTC','USDT')]


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

class OKEX_TradesFetcher(TradesFetcher):    
    def parse_response(self, data_chunk):
        data_chunk.drop('time', inplace = True, axis = 1)
        data_chunk['timestamp'] = pd.to_datetime(data_chunk['timestamp'])
        data_chunk['side'].replace('sell','s',inplace = True)
        data_chunk['side'].replace('buy','b', inplace = True)
        data_chunk['size'] = data_chunk['size'].apply(float)
        data_chunk['price'] = data_chunk['price'].apply(float)
        data_chunk['localtime'] = [datetime.now() for i in range(len(data_chunk))]
       # data_chunk['exchange'] = ['CEX' for i in range(len(data_chunk))]
        data_chunk['symbol'] = [self.symbol for i in range(len(data_chunk))]
        data_chunk['vol_curr'] = [self.vol_curr for i in range(len(data_chunk))]
        data_chunk['price_curr'] = [self.price_curr for i in range(len(data_chunk))]
        data_chunk.rename({'size':'volume'},inplace = True, axis = 1)
        self.write(data_chunk)
        

urls = ['https://www.okex.com/api/spot/v3/instruments/'+i[0]+'-'+i[1]+'/trades?limit=100' for i in symbols]
objs = [OKEX_TradesFetcher(urls[i], 'postgres','admin',o[0]+o[1], price_curr = o[1], schema = 'Okex') for i,o in enumerate(symbols)]
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
            time.sleep(2)

except KeyboardInterrupt:
            print('Execution stopped by the user')
            logging.debug('Execution stopped by the user')
          #  break