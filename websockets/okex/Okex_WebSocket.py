# -*- coding: utf-8 -*-
"""
Created on Thu Sep  2 17:09:01 2021

@author: Anar
"""
import websockets
import time
import hmac
import json
import base64
import asyncio
import websockets
from typing import Dict
from datetime import datetime
from sqlalchemy import create_engine
from sqlalchemy import exc
import pandas as pd
import logging

PAIR = ['BTC', 'USDT']
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
        self.schema = schema
        self.symbol = symbol
        self.last_time = 0
        self.backup_period = 10
        self.orderbook = {}
        self.engine = create_engine('postgresql://'+username+':'+password+'@'+host+'/'+database)
        self.columns = ['type','timestamp','localtime','snapshot',
                        'price','volume','liquidation','num_of_orders']
        for name in self.columns:
            self.orderbook[name] = []

    def push_message(self, data):
        if 'event' in data.keys():
            return
        if data['arg']['channel'] == 'books5':
            self.initial_commit(data['data'][0])
            return
        if data['action']=='snapshot':
            print(data['data'][0])
            self.initial_commit(data['data'][0])
        elif data['action']=='update':
            self.incremental_commit(data['data'][0])
    
    def initial_commit(self, data):
        for i in data['bids']:
            self.orderbook['type'].append('b')
            self.orderbook['timestamp'].append(pd.to_datetime(int(data['ts']),unit = 'ms'))
            self.orderbook['localtime'].append(datetime.now())
            self.orderbook['price'].append(float(i[0]))
            self.orderbook['volume'].append(float(i[1]))
            self.orderbook['snapshot'].append(True)
            self.orderbook['liquidation'].append(int(i[2]))
            self.orderbook['num_of_orders'].append(int(i[3]))
            
        for i in data['asks']:
            self.orderbook['type'].append('a')
            self.orderbook['timestamp'].append(pd.to_datetime(int(data['ts']),unit = 'ms'))
            self.orderbook['localtime'].append(datetime.now())
            self.orderbook['price'].append(float(i[0]))
            self.orderbook['volume'].append(float(i[1]))
            self.orderbook['snapshot'].append(True)
            self.orderbook['liquidation'].append(int(i[2]))
            self.orderbook['num_of_orders'].append(int(i[3]))
        
        self.write_to_db()
        
    def incremental_commit(self, data):
        for i in data['bids']:
            self.orderbook['type'].append('b')
            self.orderbook['timestamp'].append(pd.to_datetime(int(data['ts']),unit = 'ms'))
            self.orderbook['localtime'].append(datetime.now())
            self.orderbook['price'].append(float(i[0]))
            self.orderbook['volume'].append(float(i[1]))
            self.orderbook['snapshot'].append(False)
            self.orderbook['liquidation'].append(int(i[2]))
            self.orderbook['num_of_orders'].append(int(i[3]))
            
        for i in data['asks']:
            self.orderbook['type'].append('a')
            self.orderbook['timestamp'].append(pd.to_datetime(int(data['ts']),unit = 'ms'))
            self.orderbook['localtime'].append(datetime.now())
            self.orderbook['price'].append(float(i[0]))
            self.orderbook['volume'].append(float(i[1]))
            self.orderbook['snapshot'].append(False)
            self.orderbook['liquidation'].append(int(i[2]))
            self.orderbook['num_of_orders'].append(int(i[3]))
        
        self.write_to_db()
        
    def write_to_db(self):
        if time.time()-self.last_time>self.backup_period:
            print(pd.DataFrame(self.orderbook))
            pd.DataFrame(self.orderbook).to_sql(self.symbol, self.engine, if_exists = 'append', index = False, schema = self.schema)
            self.last_time = time.time()
            for name in self.columns:
                self.orderbook[name].clear()


class OKEX_Websocket(object):
    def __init__(self, writer,
                 api_key: str = None, api_secret: str = None, passphrase: str = None,
                 websocket=None, ws_url: str = 'wss://ws.okex.com:8443/ws/v5/public') -> None:
        self.api_key = api_key
        self.api_secret = api_secret
        self.passphrase = passphrase
        self.timestamp = (time.time())+10
        self.ws_url = ws_url
        self.get_method = "GET"
        self.auth_path = "/users/self/verify"
        self.websocket = websocket
        self.writer = writer

    # Send message wrapper
    async def send(self, message) -> None:
        await self.websocket.send(json.dumps(message))

    # Send ping
    async def ping(self):
        await self.websocket.send(str('ping'))

    # creating signature&login_params
    async def auth_params(self) -> Dict:
        message = str(self.timestamp) + self.get_method + self.auth_path

        mac = hmac.new(bytes(self.api_secret, encoding='utf8'), bytes(message, encoding='utf-8'), digestmod='sha256')
        dec = mac.digest()
        sign = base64.b64encode(dec)

        auth_msg = {"op": "login", "args": [{"apiKey": self.api_key,
                                             "passphrase": self.passphrase,
                                             "timestamp": self.timestamp,
                                             "sign": sign.decode("utf-8")}]}
        return auth_msg

    # Send authorization message
    async def authorize(self) -> None:
        await self.send(await self.auth_params())
        await asyncio.sleep(0.5)

    async def subscribe_orderbook(self):
        subscribe_tr = {
                          "op": "subscribe",
                          "args": [
                            {
                              "channel": "books5",
                              "instId": PAIR[0]+'-'+PAIR[1]
                            }
                          ]
                        }
        return await self.send(subscribe_tr)
        
    async def subscribe_account(self) -> None:
        subscribe_account_msg = {"op": "subscribe",
                                 "args": [{
                                     "channel": "account",
                                 }]
                                 }
        return await self.send(subscribe_account_msg)

    async def subscribe_positions(self, instrument_type: str = "ANY") -> None:
        subscribe_positions_msg = {"op": "subscribe",
                                   "args": [{
                                       "channel": "positions",
                                       "instType": instrument_type
                                   }]
                                   }
        return await self.send(subscribe_positions_msg)

    async def subscribe_orders(self, instrument_type: str = "ANY") -> None:
        subscribe_orders_msg = {"op": "subscribe",
                                "args": [{
                                    "channel": "orders",
                                    "instType": instrument_type
                                }]
                                }
        return await self.send(subscribe_orders_msg)

    # Listen ws answers
    async def listen(self) -> Dict:
        response = None
        while response is None:
            response = json.loads(await self.websocket.recv())
        self.process_event(response)
        return response

    async def __aenter__(self):
        self.websocket = await websockets.connect(self.ws_url)
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.websocket.close()
    
    def process_event(self, response):
        if response['arg']['channel'] == 'books5' or response['arg']['channel'] == 'books':
            writer.push_message(response)
            

if __name__ == '__main__':
        writer = OrderBook_Writer('postgres','admin','Orderbook','Okex')
        socket = OKEX_Websocket(writer = writer, api_key = 'f1ce2f8f-59ef-4e7d-9287-eeddaa438f6c',
                                api_secret = 'CBD7151C246E935A570BD1A1BE22C1DC',
                                passphrase = '1q2w3e4rq')
        
        loops = asyncio.get_event_loop()
        loops.run_until_complete(socket.__aenter__())
        loops.run_until_complete(socket.subscribe_orderbook())
        while True:
            try:
                loops.run_until_complete(socket.listen())
            except Exception as ex:
                logger.error(type(ex))
else:
    pass