import asyncio
import collections
import datetime
import logging
import os
import pickle
import pprint
import shelve
import json

import bson
import crayons

import pymake.registry
import pymake.registry.r1.address
from pymake.util import clean

def breakpoint(): import pdb; pdb.set_trace()

logger = logging.getLogger(__name__)
logger_get_id = logging.getLogger(__name__+"-get-id")

def get_subregistry(r, l, f=None):

    assert isinstance(l, list)

    assert isinstance(r, (shelve.Shelf, SubRegistry))

    for k in l:

        if k not in r:
            r[k] = SubRegistry()
        
        try:
            r = r[k]
        except AttributeError as e:
            # should be unpickling error
            print(crayons.red(f'get_subregistry l={l!r}'))
            print(crayons.red(f'deleting branch'))

            r[k] = SubRegistry()
            r = r[k]
            

        # created for debugging
        if f is not None: f(r)

        assert isinstance(r, SubRegistry)

    return r
 

async def get_id(d):

        logger_get_id.debug(f'{d!r}')

        assert isinstance(d, dict)

        d = clean(d)
 
        d_1 = {"doc": d}

        docs = await pymake.client.client.find(d_1).to_list(2)

        if len(docs) > 1:

            print()

            for d in docs:
                print('doc:')
                print(f'  id: {d["_id"]}')
                for k, v in d['doc'].items():
                    print(f'    {k!r} {v!r}')
                print()
    
            print()

            if docs[0]['doc']['type_'] in (
                    'data_file',
                    'data file builder',
                    'fin surface',
                    'fin surface input',
                    'csv row',
                    'coil, csv',
                    ):
                print(crayons.red("delete"))
                pymake.client.client._coll.delete_many(d_1)
                

            raise Exception(f"got multiple db records for {json.dumps(d_1)!r}")

        doc = await pymake.client.client.find_one(d_1)

        if doc is None:
            logger.debug(f'did not find {d_1}. inserting')
            res = await pymake.client.client.insert_one(d_1)
            return res.inserted_id

        return doc["_id"]

class Registry(pymake.registry.Registry):

    def __init__(self, filename, filename_meta):
        super().__init__()

        self._db = shelve.open(filename, writeback=True)
        self._db_meta = shelve.open(filename_meta, writeback=True)

    async def get_id_cached(self, req):

        if not hasattr(req, "_id_CACHED"):

            req._id_CACHED = await self.get_id(req)

        return req._id_CACHED

    async def get_id(self, req):

        return await get_id(req.encoded)

    async def get_lock(self, req):

        _id = await self.get_id_cached(req)

        logger.debug(f'_id = {_id!s}')

        async with self.__lock:
            if _id not in self._locks:
                self._locks[_id] = asyncio.Lock()

            return self._locks[_id]


    async def get_subregistry(self, req, f=None):

        d = clean(req.encoded)
        _id = await self.get_id_cached(req)

        logger.debug(f'req: {req}')
        logger.debug(f'_id: {_id!s}')


        if str(_id) not in self._db:
            self._db[str(_id)] = pymake.registry.SubRegistry()

        try:
            return self._db[str(_id)]
        except (AttributeError, ModuleNotFoundError) as e:
            # indicates problem with pickled object, need to delete it
            logger.error(crayons.red(f'Unpickle error for {_id}. delete'))
            breakpoint()
            del self._db[str(_id)]
            raise
        
        except RuntimeError as exc:

            if exc.args[0] == "input stream error":
                logger.error(crayons.red(f'Unpickle error for {_id}. delete'))
                breakpoint()
                del db[str(_id)]

            #breakpoint()
            #
            raise

    async def get_subregistry_meta(self, req, f=None):

        #d = clean(req.encoded)
        _id = await self.get_id_cached(req)

        logger.debug(f'req: {req}')
        logger.debug(f'_id: {_id!s}')

        i = str(_id)

        if i not in self._db_meta:
            logger.info(f'new subregistry for {req}')
            self._db_meta[i] = pymake.doc_registry.SubRegistry()

        try:
            return self._db_meta[str(_id)]

        except ModuleNotFoundError as exc:

            breakpoint()

            raise


    def close(self):
        logger.info(crayons.blue("close registry"))
        self._db.close()
        self._db_meta.close()


