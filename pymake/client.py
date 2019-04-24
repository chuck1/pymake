import contextlib
import datetime
import enum
import functools
import inspect
import pickle
import re
import os
import logging
import time
import traceback
import pymongo
import bson
import json
import pprint

import motor.motor_asyncio
import cached_property
from mybuiltins import *

from pymake.exceptions import *
from pymake.util import *
from pymake.result import *
from pymake.fakepickle import *
import pymake

#import pymake.doc_registry

logger = logging.getLogger(__name__)
logger_pickle = logging.getLogger(__name__ + '-pickle')
logger_mongo  = logging.getLogger(__name__ + '-mongo')

class Client:
    def __init__(self):
        #client = pymongo.MongoClient()
        client = motor.motor_asyncio.AsyncIOMotorClient()
        db = client.coiltest
        self._coll = db.test

    def exists(self, q):
        c = self.coll.find(q).limit(1)
        try:
            next(c)
            return True
        except StopIteration:
            return False

    def get_desc(self, _id):
        if isinstance(_id, str):
            _id = bson.objectid.ObjectId(_id)
        assert isinstance(_id, bson.objectid.ObjectId)
        d = self.find_one({"_id": _id})
        def f(s):
            if s.startswith("_"): return False
            return True
        d = dict((k, v) for k, v in d.items() if f(k))
        return d

    def find(self, q):
        doc = self._coll.find(q)
        return doc

    async def find_one(self, q):
        logger.debug(f"q = {q!r}")

        if "type" in q: 
            if q["type"] in ["node 90",]: raise Exception()

        doc = await self._coll.find_one(q)

        logger.debug(f'doc = {doc!s:.32s}')

        return doc

    async def insert_one(self, q):
        try:
            return await self._coll.insert_one(q)
        except:
            logger.error(crayons.red('error in mongo insert'))
            pprint.pprint(q)
            raise

    async def update_one(self, q, u):
        if '$set' not in u:
            u['$set'] = {}

        d = await self._coll.find_one(q)

        if d is None:
            res = self._coll.insert_one(q)
            _id = res.inserted_id
        else:
            _id = d["_id"]

        logger.debug(crayons.green(f'write binary: {_id}'))

        t = datetime.datetime.now()

        u['$set']['_last_modified'] = t
        
        await self._coll.update_one({"_id": _id}, u)
        
        return t

    def resolve(self, q):
        raise Exception()

        docs = list(self._coll.find(q))
        if len(docs) < 2: return

        for i, d in zip(range(len(docs)), docs):
            self._coll.update_one({"_id": d["_id"]}, {"$set": {"RESOLVE": i}})


client = Client()






