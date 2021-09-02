# -*- coding: utf-8 -*-
"""
Created on Thu Sep  2 11:40:11 2021

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
from TradesFetcher import TradesFetcher

logname = 'CEX_TradesFetcher'
symbols = [('ADA','USDT'),('XRP','USDT'),
           ('BTC','USDT'),('ETH','USDT'),
           ('XLM','USDT'),('BCH', 'USDT'),
           ('BAT', 'USDT'),('LTC','USDT'),
           ('ADA','USD'),('XRP','USD'),
           ('BTC','USD'),('ETH','USD'),
           ('XLM','USD'),('BCH', 'USD'),
           ('BAT', 'USD'),('LTC','USD')]

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


urls = ['https://cex.io/api/trade_history/'+i[0]+'/'+i[1]+'/' for i in symbols]
objs = [CEX_TradesFetcher(urls[i], 'postgres','admin',o[0]+o[1], schema = 'CEX', price_curr = o[1]) for i,o in enumerate(symbols)]
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
          #  break