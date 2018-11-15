import asyncio
import datetime
import logging
import os
import pickle
import shelve

import crayons
import pymake.doc_registry.address

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
        doc = pymake.client.client.find_one(d)

        #self._mtime = self._read_mtime(d)

        if doc is None:
            res = pymake.client.client.insert_one(d)
            return str(res.inserted_id)

        return str(doc["_id"])

def _lock(f):
    async def wrapped(self, d, *args):

        _id = get_id(d)

        l = await self.get_lock(_id)

        #print('_lock', type(self), type(d), _id, l)
 
        logger.debug('aquire lock')
        async with l:
            logger.debug('lock aquired')
            return await f(self, d, *args)

    return wrapped

METHOD = "ID"

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
        logger.warning(crayons.yellow("delete"))

        r = self.get_subregistry(d)
        
        r.doc = None

    def get_subregistry(self, d, f=None):
        if METHOD == "ADDRESS":
            a = pymake.doc_registry.address.Address(d)
            r = self._registry
            r = get_subregistry(r, a.l, f=f)
            return r
        else:
            db = self._registry
            _id = get_id(d)
            if _id not in db: db[_id] = SubRegistry()
            return db[_id]

    def get_subregistry_meta(self, d, f=None):
        if METHOD == "ADDRESS":
            a = pymake.doc_registry.address.Address(d)
            r = self._db_meta
            r = get_subregistry(r, a.l, f=f)
            return r
        else:
            _id = get_id(d)
            if _id not in self._db_meta: self._db_meta[_id] = SubRegistry()
            return self._db_meta[_id]

    @_lock
    async def exists(self, d):
        return await self.__exists(d)

    async def __exists(self, d):
        r = self.get_subregistry_meta(d)
        if not hasattr(r, "doc"): return False
        if r.doc is None: return False

        r1 = self.get_subregistry(d)
        if not hasattr(r1, "doc"): return False
        if r1.doc is None: return False

        return True

    @_lock
    async def read(self, d):

        if not (await self.__exists(d)): raise Exception(f"Object not found: {d!r}")

        r = self.get_subregistry(d)

        logger.debug(f"read from {type(r)} {id(r)}")

        if r.doc is None:
            raise Exception(f"Object not found: {d!r}")

        return r.doc.doc

    @_lock
    async def read_mtime(self, d):
        r = self.get_subregistry_meta(d)
        return r.doc.mtime

    @_lock
    async def write(self, d, doc):

        assert not asyncio.iscoroutine(doc)

        r = self.get_subregistry(d)
        r_meta = self.get_subregistry_meta(d)

        r.doc = Doc(doc, d=d)
        r_meta.doc = DocMeta(d=d)

    def DEPdump(self):
        try:
            s = pickle.dumps(self._registry)
        except:
            self.test_pickle([], self._registry)
            raise

        with open("build/doc_registry.bin", "wb") as f:
            f.write(s)
       
    def DEPtest_pickle(self, l, r):
        if hasattr(r, "doc"):
            if r.doc:
                try:
                    pickle.dumps(r.doc)
                except Exception as e:
                    logger.error(repr(l))
                    logger.error(repr(r.doc))
                    logger.error(repr(e))
    
                    for k, v in r.doc.__dict__.items():
                        try:
                            pickle.dumps(v)
                        except Exception as e:
                            logger.error(repr(k))
                            logger.error(repr(v))
                            logger.error(repr(e))

        for k, v in r.d.items():
            self.test_pickle(l + [k], v)

       
class Registry2:
    def __init__(self, db):

        self.address_book = db

    def next_id(self):
        c = self.address_book.get("counter", 0)
        self.address_book["counter"] = c + 1
        return int2str(c, 36)

    def get_filename(self, d):
        a = pymake.doc_registry.address.Address(d)

        pre = "build/registry"

        if s in self.address_book:
            return os.path.join(pre, self.address_book[s])
        else:
            filename = self.next_id()
            self.address_book[s] = filename
            return os.path.join(pre, filename)

    def delete(self, d):
        logger.warning(crayons.yellow("delete"))
        s = self.get_filename(d)
        os.remove(s)

    def exists(self, d):
        return os.path.exists(self.get_filename(d))

    def read(self, d):
        s = self.get_filename(d)

    def read_mtime(self, d):

        r = self.get_subregistry(d)

        return r.doc.mtime

    def write(self, d, doc):

        r = self.get_subregistry(d)

        r.doc = Doc(doc)

class DocRegistry2(DocRegistry):

    def get_subregistry(self, d, f=None):
        
        a = pymake.doc_registry.address.Address(d)

        k = str(a.h) #a.s

        if k not in self._registry:
            self._registry[k] = SubRegistry()

        r = self._registry[k]

        return r


