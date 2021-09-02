# -*- coding: utf-8 -*-
"""
Created on Thu Sep  2 13:24:50 2021

@author: Anar
"""

import pandas as pd
import time
import logging
from datetime import datetime
from TradesFetcher import TradesFetcher
import urllib
import json

logname = 'Coinbase_TradesFetcher'
symbols = [('ADA','USD'),('BTC','USD'), ('ETH','USD'),
           ('BTC','USDT'),('ETH','USDT'),
           ('XLM','USD'),('BCH', 'USD'),
           ('LTC', 'USD'),('BAT', 'USD')]

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

class COINBASE_TradesFetcher(TradesFetcher):  
    def check_response(self, response):
        try:
            J = json.loads(response.read())
        except:
            assert False, 'A problem with json parsing has occured, response:{resp}'.format(resp = response.read())
        assert  len(J) != 0, 'Response is empty'
        data_chunk = pd.DataFrame(J)
        #print(data_chunk)
        self.parse_response(data_chunk)
        
    def parse_response(self, data_chunk):
        
        data_chunk.rename({'time':'timestamp','size':'volume'},axis = 1, inplace = True)
        data_chunk['symbol'] = [self.symbol for i in range(len(data_chunk))]
        data_chunk['price_curr'] = [self.price_curr for i in range(len(data_chunk))]
        data_chunk['vol_curr'] = [self.vol_curr for i in range(len(data_chunk))]
        data_chunk.replace({'buy':'b','sell':'s'}, inplace = True)
        data_chunk['timestamp'] = pd.to_datetime(data_chunk['timestamp'])
        data_chunk['price'] = data_chunk['price'].apply(float)
        data_chunk['volume'] = data_chunk['volume'].apply(float)
        data_chunk['localtime'] = [datetime.now() for i in range(len(data_chunk))]
        self.write(data_chunk)
        #data_chunk[['price','volume']].astype(float)
        #self.write(data_chunk)
        #self.parse_response(data_chunk)        
        
urls = ['https://api.pro.coinbase.com/products/'+i[0]+'-'+i[1]+'/trades' for i in symbols]
objs = [COINBASE_TradesFetcher(urls[i], 'postgres','admin',o[0]+o[1], schema = 'Coinbase', price_curr = o[1]) for i,o in enumerate(symbols)]
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
            time.sleep(5)

except KeyboardInterrupt:
            print('Execution stopped by the user')
            logging.debug('Execution stopped by the user')