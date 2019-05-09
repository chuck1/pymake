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
import pymake.doc_registry.address
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
 
class SubRegistry:
    def __init__(self):
        self.d = {}
        self.doc = None

    def items(self):
        return self.d.items()

    def __iter__(self):
        return iter(self.d)

    def __setitem__(self, key, value):
        self.d[key] = value

    def __getitem__(self, key):
        return self.d[key]

    def __str__(self):
        return str(self.d)

    def __repr__(self):
        if self.doc is not None:
            return f"Sub({self.d}, doc={self.doc})"
        else:
            return f"Sub({self.d})"

    def __getstate__(self):
        state = dict(self.__dict__)
        if "doc" in state:
            if state["doc"] is not None:
                try:
                    pickle.dumps(state["doc"])
                except Exception as e:
                    logger.warning(crayons.yellow(f'error pickling {state["doc"]} {e!r}'))
                    #logger.warning(crayons.yellow(repr(e)))
                    del state["doc"]
        return state

class Doc:
    def __init__(self, doc, d=None):
        self.doc = doc
        self.d = d
        self.mtime = datetime.datetime.now().timestamp()

class DocMeta:
    def __init__(self, d=None):
        self.d = d
        self.mtime = datetime.datetime.now().timestamp()

async def get_id(d):

        logger_get_id.debug(f'{d!r}')

        assert isinstance(d, dict)

        d = clean(d)
 
        #d_1 = d
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

def _lock(f):
    async def wrapped(self, _id, d, *args):
        #d = clean(d)

        assert isinstance(_id, bson.objectid.ObjectId)

        l = await self.get_lock(_id)
 
        logger.debug('aquire lock')
        async with l:
            logger.debug('lock aquired')
            return await f(self, _id, d, *args)

    return wrapped

class DocRegistry:

    def __init__(self, db, db_meta):

        self._registry = db
        self._db_meta = db_meta

        self._locks = {}

        self.__lock = asyncio.Lock()

    async def get_lock(self, _id):
        logger.debug(f'_id = {_id!s}')

        async with self.__lock:
            if _id not in self._locks:
                self._locks[_id] = asyncio.Lock()

            return self._locks[_id]

    @_lock
    async def delete(self, _id, d):
        assert isinstance(_id, bson.objectid.ObjectId)
        d = clean(d)
        await self.__delete(d)

    async def __delete(self, d):
        logger.warning(crayons.yellow(f"delete {d}"))

        d = clean(d)

        r = await self.get_subregistry(d)
        
        r.doc = None

    async def get_subregistry(self, _id, d, f=None):
        d = clean(d)

        db = self._registry
        
        if str(_id) not in db:
            db[str(_id)] = SubRegistry()

        try:
            return db[str(_id)]
        except (AttributeError, ModuleNotFoundError) as e:
            # indicates problem with pickled object, need to delete it
            logger.error(crayons.red(f'Unpickle error for {_id}. delete'))
            del db[str(_id)]
            raise
        
        except RuntimeError as exc:

            if exc.args[0] == "input stream error":
                logger.error(crayons.red(f'Unpickle error for {_id}. delete'))
                del db[str(_id)]

            #breakpoint()
            #
            raise

    def get_subregistry_meta(self, _id, d, f=None):
        d = clean(d)

        if str(_id) not in self._db_meta:
            self._db_meta[str(_id)] = SubRegistry()

        return self._db_meta[str(_id)]

    @_lock
    async def exists(self, _id, d):
        assert isinstance(_id, bson.objectid.ObjectId)
        d = clean(d)
        return await self.__exists(_id, d)

    async def __exists(self, _id, d):
        d = clean(d)

        r = self.get_subregistry_meta(_id, d)

        if not hasattr(r, "doc"): 
            logger.info('subregistry_meta does not have "doc" attribute')
            return False

        if r.doc is None: 
            logger.info('subregistry_meta "doc" is None')
            return False

        r1 = await self.get_subregistry(_id, d)

        if not hasattr(r1, "doc"): 
            logger.info('subregistry does not have "doc" attribute')
            return False

        if r1.doc is None: 
            logger.info('subregistry "doc" is None')
            return False

        return True

    @_lock
    async def read(self, _id, d):
        assert isinstance(_id, bson.objectid.ObjectId)
        d = clean(d)

        logger.debug(f"read {_id} {d}")

        if not (await self.__exists(_id, d)): 
            raise Exception(f"Object not found: {repr(d)[:1000]}")

        r = await self.get_subregistry(_id, d)

        logger.debug(f"read from {type(r)} {id(r)}")

        if r.doc is None:
            raise Exception(f"Object not found: {d!r}")

        return r.doc.doc

    @_lock
    async def read_mtime(self, _id, d):
        assert isinstance(_id, bson.objectid.ObjectId)
        d = clean(d)
        r = self.get_subregistry_meta(_id, d)
        return r.doc.mtime

    @_lock
    async def write(self, _id, d, doc):
        assert isinstance(_id, bson.objectid.ObjectId)
        d = clean(d)


        logger.debug(f"write {_id!r} {d!r} {doc!r}")

        assert not asyncio.iscoroutine(doc)

        r = await self.get_subregistry(_id, d)
        r_meta = self.get_subregistry_meta(_id, d)

        r.doc = Doc(doc, d=d)
        r_meta.doc = DocMeta(d=d)


