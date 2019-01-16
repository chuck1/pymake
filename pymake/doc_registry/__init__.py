import asyncio
import collections
import datetime
import logging
import os
import pickle
import shelve

import crayons
import pymake.doc_registry.address
from pymake.util import clean

logger = logging.getLogger(__name__)

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

def get_id(d):
        d = clean(d)
        
        docs = pymake.client.client.find(d)

        if len(list(docs)) > 1:
            raise Exception(f"got multiple db records for {d!r}")

        doc = pymake.client.client.find_one(d)


        if doc is None:
            res = pymake.client.client.insert_one(d)
            return str(res.inserted_id)

        return str(doc["_id"])

def _lock(f):
    async def wrapped(self, d, *args):
        d = clean(d)

        _id = get_id(d)

        l = await self.get_lock(_id)

 
        logger.debug('aquire lock')
        async with l:
            logger.debug('lock aquired')
            return await f(self, d, *args)

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
    async def delete(self, d):
        d = clean(d)
        await self.__delete(d)

    async def __delete(self, d):
        logger.warning(crayons.yellow("delete"))

        d = clean(d)

        r = await self.get_subregistry(d)
        
        r.doc = None

    async def get_subregistry(self, d, f=None):
        d = clean(d)

        db = self._registry
        _id = get_id(d)
        if _id not in db:
            db[_id] = SubRegistry()

        try:
            return db[_id]
        except AttributeError as e:
            # indicates problem with pickled object, need to delete it
            logger.error(crayons.red(f'Unpickle error for {_id}. delete'))
            del db[_id]
            raise

    def get_subregistry_meta(self, d, f=None):
        d = clean(d)

        _id = get_id(d)
        
        if _id not in self._db_meta:
            self._db_meta[_id] = SubRegistry()

        return self._db_meta[_id]

    @_lock
    async def exists(self, d):
        d = clean(d)
        return await self.__exists(d)

    async def __exists(self, d):
        d = clean(d)
        r = self.get_subregistry_meta(d)
        if not hasattr(r, "doc"): return False
        if r.doc is None: return False

        r1 = await self.get_subregistry(d)
        if not hasattr(r1, "doc"): return False
        if r1.doc is None: return False

        return True

    @_lock
    async def read(self, d):
        d = clean(d)

        _id = get_id(d)

        logger.debug(f"read {_id} {d}")

        if not (await self.__exists(d)): 
            raise Exception(f"Object not found: {d!r}")

        r = await self.get_subregistry(d)

        logger.debug(f"read from {type(r)} {id(r)}")

        if r.doc is None:
            raise Exception(f"Object not found: {d!r}")

        return r.doc.doc

    @_lock
    async def read_mtime(self, d):
        d = clean(d)
        r = self.get_subregistry_meta(d)
        return r.doc.mtime

    @_lock
    async def write(self, d, doc):
        d = clean(d)

        _id = get_id(d)

        logger.debug(f"write {_id} {d}")

        assert not asyncio.iscoroutine(doc)

        r = await self.get_subregistry(d)
        r_meta = self.get_subregistry_meta(d)

        r.doc = Doc(doc, d=d)
        r_meta.doc = DocMeta(d=d)


