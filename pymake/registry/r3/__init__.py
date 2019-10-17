import asyncio
import collections
import datetime
import logging
import os
import pickle
import pprint
import shelve
import json
import struct

import bson
import crayons
import pymake.registry
from pymake.util import clean
import pymake.registry

logger = logging.getLogger(__name__)

class EchoClientProtocol(asyncio.Protocol):

    def __init__(self, loop):
        self.loop = loop
        self.i = 0
        self.futures = dict()

        self.length = None
        self.buffer = b""

    def connection_made(self, transport):
        self.transport = transport
        #transport.write(self.message.encode())
        #print('Data sent: {!r}'.format(self.message))

    def data_received(self, data):
        logger.debug(f"received {len(data)} bytes")

        if self.length == None:
            self.length = struct.unpack('i', data[:4])[0]
            logger.debug(f"message length: {self.length}")
            data = data[4:]
        
        self.buffer = self.buffer + data

        if len(self.buffer) < self.length:
            return

        if len(self.buffer) > self.length:
            raise Exception("buffer has too much data")

        logger.debug("full message received")

        message = pickle.loads(self.buffer)

        self.futures[message.response_to].set_result(message)

        # prepare for next message
        self.length = None
        self.buffer = b""

    def connection_lost(self, exc):
        logger.debug('The server closed the connection')
        self.on_con_lost.set_result(True)

    def send(self, message):
        message.i = self.i
        self.i += 1
        s = pickle.dumps(message)
        
        self.futures[message.i] = self.loop.create_future()
        
        self.transport.write(struct.pack('i', len(s)))
        self.transport.write(s)

        return self.futures[message.i]

class MessageReturn:
    def __init__(self, response_to, ret):
        self.response_to = response_to
        self.ret = ret

class MessageExists:
    def __init__(self, req):
        self.req = req

    async def get_ret(self, server):
        return await server.registry.exists(self.req)

    def __repr__(self):
        return f"<{self.__class__.__name__:20} {self.req!r}>"

class MessageRead:
    def __init__(self, req):
        self.req = req
    async def get_ret(self, server):
        return await server.registry.read(self.req)

class MessageReadMTime:
    def __init__(self, req):
        self.req = req
    async def get_ret(self, server):
        return await server.registry.read_mtime(self.req)

class MessageWrite:
    def __init__(self, req, o):
        self.req = req
        self.o = o

    async def get_ret(self, server):
        return await server.registry.write(self.req, self.o)

class MessageGetSubregistry:
    def __init__(self, req):
        self.req = req

class MessageGetSubregistryMeta:
    def __init__(self, req):
        self.req = req

class Registry(pymake.registry.Registry):
    """
    this version of registry will open a connection to a registry server
    """

    def __init__(self, loop):
        super().__init__()
        self.protocol = None
        self.loop = loop
    
    async def ainit(self):
        transport, self.protocol = await self.loop.create_connection(
                lambda: EchoClientProtocol(self.loop),
                '127.0.0.1', 
                8888)

    async def get_lock(self, req):
        raise NotImplementedError()
        i = req.hash2

        logger.debug(f'i = {i!s}')

        async with self.__lock:

            if i not in self._locks:
                
                self._locks[i] = asyncio.Lock()

            return self._locks[i]

    async def get_subregistry(self, req, f=None):
        if self.protocol is None: await self.ainit()

        message = MessageGetSubregistry(req)

        response = await self.protocol.send(message)

        return response.ret

    async def get_subregistry_meta(self, req, f=None):
        if self.protocol is None: await self.ainit()
        raise NotImplementedError()

    def close(self):
        pass
        #raise NotImplementedError()

    async def exists(self, req):
        message = MessageExists(req)
        response = await self.protocol.send(message)
        return response.ret

    async def write(self, req, o):
        message = MessageWrite(req, o)
        response = await self.protocol.send(message)
        return response.ret

    async def read(self, req):
        message = MessageRead(req)
        response = await self.protocol.send(message)
        return response.ret

    async def read_mtime(self, req):
        message = MessageReadMTime(req)
        response = await self.protocol.send(message)
        return response.ret











