import asyncio
import asyncio.coroutines
import logging
import random
import sys

from cexio.exceptions import *
from cexio.messaging import *
from cexio.ws_client import *

from config.my_config import config

import pandas as pd
import time 
from datetime import datetime 

from sqlalchemy import create_engine
from sqlalchemy import exc

PAIR = ['BTC','USD']

logname = "Orderbook_Fetcher"+PAIR[0]+PAIR[1]
logger = logging.getLogger("err")
formatter = logging.Formatter('%(asctime)s.%(msecs)d,%(levelname)s:%(message)s',
                              datefmt='%Y-%m-%d %H:%M:%S')
file_handler = logging.FileHandler(filename=logname+"_error.log")
file_handler.setFormatter(formatter)
file_handler.setLevel(logging.ERROR)
logger.addHandler(file_handler)

class OrderBook_Writer():
    def __init__(self, username, password, symbol, 
                 schema, database = 'Ticks', host = 'localhost:5432'):
        self.last_id = 0
        self.schema = schema
        self.symbol = symbol
        self.last_time = 0
        self.backup_period = 10
        self.orderbook = {}
        self.engine = create_engine('postgresql://'+username+':'+password+'@'+host+'/'+database)
        self.columns = ['type','timestamp','localtime','snapshot','price','volume','id']
        for name in self.columns:
            self.orderbook[name] = []
       # self.orderbook = pd.DataFrame(self.orderbook)
            
    def push_message(self, data):
        if data['e']=='order-book-subscribe':
            self.initial_commit(data['data'])
        elif data['e']=='md_update':
            self.incremental_commit(data['data'])
    
    def initial_commit(self, data):
        self.last_id = int(data['id'])
        
        for i in data['bids']:
            self.orderbook['type'].append('b')
            self.orderbook['timestamp'].append(pd.to_datetime(int(data['timestamp']),unit = 's'))
            self.orderbook['localtime'].append(datetime.now())
            self.orderbook['price'].append(float(i[0]))
            self.orderbook['volume'].append(float(i[1]))
            self.orderbook['snapshot'].append(True)
            self.orderbook['id'].append(self.last_id)
            
        for i in data['asks']:
            self.orderbook['type'].append('a')
            self.orderbook['timestamp'].append(pd.to_datetime(int(data['timestamp']),unit = 's'))
            self.orderbook['localtime'].append(datetime.now())
            self.orderbook['price'].append(float(i[0]))
            self.orderbook['volume'].append(float(i[1]))
            self.orderbook['snapshot'].append(True)
            self.orderbook['id'].append(self.last_id)
        self.write_to_db()
        
    def incremental_commit(self, data):
        self.last_id = int(data['id'])
        for i in data['bids']:
            self.orderbook['type'].append('b')
            self.orderbook['timestamp'].append(pd.to_datetime(int(data['time']),unit = 'ms'))
            self.orderbook['localtime'].append(datetime.now())
            self.orderbook['price'].append(float(i[0]))
            self.orderbook['volume'].append(float(i[1]))
            self.orderbook['snapshot'].append(False)
            self.orderbook['id'].append(self.last_id)
            
        for i in data['asks']:
            self.orderbook['type'].append('a')
            self.orderbook['timestamp'].append(pd.to_datetime(int(data['time']),unit = 'ms'))
            self.orderbook['localtime'].append(datetime.now())
            self.orderbook['price'].append(float(i[0]))
            self.orderbook['volume'].append(float(i[1]))
            self.orderbook['snapshot'].append(False)
            self.orderbook['id'].append(self.last_id)
        
        self.write_to_db()
        
    def write_to_db(self):
        if time.time()-self.last_time>self.backup_period:
            print(pd.DataFrame(self.orderbook))
            pd.DataFrame(self.orderbook).to_sql(self.symbol, self.engine, if_exists = 'append', index = False, schema = self.schema)
              
            self.last_time = time.time()
            self.columns = ['type','timestamp','localtime','snapshot','price','volume','id']
            for name in self.columns:
                self.orderbook[name].clear()

class CEX_Websocket(CommonWebSocketClient):
	def __init__(self, _config, writer):
		super().__init__(_config)

		def validator(m):
			try:
				ok = m['ok']
				if ok == 'ok':
					return m['data']
				elif ok == 'error':
					raise ErrorMessage(m['data']['error'])
				else:
					error = InvalidMessage(m)
			except KeyError:
				error = InvalidMessage(m)
			raise error

		resolver = RequestResponseFutureResolver(name='', op_name_get_path='e',
												 key_set_path='oid', key_get_path='oid')
		self.message_map = (
            ({	'e': 'order-book-subscribe', }, self.on_notification),
			({	'e': None,
				'data': None,
				'oid': None,
				'ok': None, }, resolver + validator),
           
           ({	'e': 'md_update', }, self.on_notification),
		)
		router = MessageRouter(self.message_map, sink=self.on_unhandled)
		self.set_router(router)
		self.set_resolver(resolver)
		self.__n_future = None
		self.__n_cond = asyncio.Condition()
		self.writer = writer

	async def on_notification(self, data):
		print(data)
		writer.push_message(data)
		return data

	@staticmethod
	def format_message(op_name, data=None):
		message = {'e': op_name, }
		if data is not None:
			message['data'] = data
		return message

	@staticmethod
	async def on_error(message):
		print("Error: ", message)

	@staticmethod
	async def on_unexpected_response(message):
		logger.debug("Resolver not expected response: {}".format(message))
		return message  # pass back to the router to continue matching

	@staticmethod
	async def on_unhandled(message):
		logger.debug("Unhandled message: {}".format(message))
		print("Unhandled: {}:".format(message))


if __name__ == "__main__":
    writer = OrderBook_Writer('postgres','admin','Orderbook6','CEX')
    client = CEX_Websocket(config, writer)
    
    async def unsubscribe_orderbook(): 
            await client.request_subscribe(client.format_message(
                				'order-book-unsubscribe',
                				{
                					'pair': [
                						PAIR[0],
                						PAIR[1],
                					],
                				}))
            
    async def justwait():
        await asyncio.sleep(300)
        
    async def fetch_orderbook(): 
            await client.request_subscribe(client.format_message(
                				'order-book-subscribe',
                				{
                					'pair': [
                						PAIR[0],
                						PAIR[1],
                					],
                					'subscribe': 'true',
                					'depth': 1,
                				}))

    loop = asyncio.get_event_loop()
    loop.set_debug(True)
    loop.run_until_complete(client.run())
    tasks = [
		asyncio.ensure_future(fetch_orderbook()),
	]
    while(True):
        try:
            loop.run_until_complete(asyncio.ensure_future(fetch_orderbook()))
            loop.run_until_complete(asyncio.ensure_future(justwait()))
            loop.run_until_complete(asyncio.ensure_future(unsubscribe_orderbook()))
        except Exception as ex:
            logger.error(type(ex))
    loop.close()

else:
	pass


