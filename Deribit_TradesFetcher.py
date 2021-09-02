# -*- coding: utf-8 -*-
"""
Created on Thu Sep  2 14:14:28 2021

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

logname = 'Deribit_TradesFetcher'
symbols = ['BTC-PERPETUAL', 'ETH-PERPETUAL']

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
class DERIBIT_TradesFetcher(TradesFetcher):  
    def check_response(self, response):
#        print(response.status_code)
        try:
            J = json.loads(response.read())
            J = J['result']['trades']
        except:
            assert False, 'A problem with json parsing has occured, response:{resp}'.format(resp = response.read())
        assert  len(J) != 0, 'Response is empty'
        data_chunk = pd.DataFrame(J)
       # print(data_chunk.columns)
        self.parse_response(data_chunk)
        
    def parse_response(self, data_chunk):
       # print(data_chunk.columns)
        data_chunk.drop(['trade_seq','tick_direction'],axis = 1,inplace = True)
        try:
            data_chunk.drop('liquidation','tick_direction',axis = 1,inplace = True)
        except:
            pass
        data_chunk.rename({'instrument_name':'symbol', 'amount':'volume','direction':'side'},axis = 1, inplace = True)
        data_chunk['symbol'] = [self.symbol for i in range(len(data_chunk))]
        data_chunk['price_curr'] = [self.price_curr for i in range(len(data_chunk))]
        data_chunk['vol_curr'] = [self.vol_curr for i in range(len(data_chunk))]
        data_chunk.replace({'buy':'b','sell':'s'}, inplace = True)
        data_chunk['timestamp'] = pd.to_datetime(data_chunk['timestamp'], unit = 'ms')
        data_chunk['price'] = data_chunk['price'].apply(float)
        data_chunk['volume'] = data_chunk['volume'].apply(float)
        data_chunk['localtime'] = [datetime.now() for i in range(len(data_chunk))]
        self.write(data_chunk)    
        
urls = ['https://www.deribit.com/api/v2/public/get_last_trades_by_instrument?count=1000&instrument_name='+i for i in symbols]
objs = [DERIBIT_TradesFetcher(urls[i], 'postgres','admin',o, schema = 'Deribit', price_curr = 'USD', vol_curr = 'CONTRACT') for i,o in enumerate(symbols)]
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