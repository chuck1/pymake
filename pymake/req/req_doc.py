import asyncio
import copy
import logging
import pickle
import pprint

import cached_property
import crayons
import bson
from mybuiltins import *

import pymake.req
import pymake.doc_registry
import pymake.util
import pymake.fakepickle
import pymake.client
from pymake.req import FakePickle

logger = logging.getLogger(__name__)


class ReqDocBase(pymake.req.Req):
    def __init__(self, d, build=True, **kwargs):
        """
        d     - bson-serializable object. once initialized, MUST NOT CHANGE. has already been decoded
        build - flag is this should be built or just read
        """
        super().__init__(**kwargs)

        if not isinstance(d, dict):
            raise Exception(f'expected dict, not {type(d)} {d!r}')

        if 'type' not in d:
            raise Exception(f'"type" not in {d!r}')

        self.d = copy.deepcopy(d)

        self.build = build

    def copy(self, d0):
        d1 = copy.deepcopy(self.d)
        d1.update(d0)
        return self.__class__(d1)

    def __deepcopy__(self, memo):
        #print(f"{self.__class__.__name__} deepcopy")
        return self
        #return self.__class__(copy.deepcopy(self.d, memo))

    async def get_rule(self, mc):
        return await mc.makefile.find_one(mc, self)

    def __repr__(self):
        if 'type' not in self.d:
            print(self.d)
            breakpoint()
        d = {"type":self.d["type"]}

        if "condition" in self.d:
            d["condition"] = self.d["condition"]

        return f'{self.__class__.__name__}({d!r})'

    def print_long(self):
        s = bson.json_util.dumps(self.encoded)
        print('d=')
        pprint.pprint(self.d)
        print(s)
        pprint.pprint(dict((k, type(v)) for k, v in self.encoded.items()))
        mypprint(self.encoded)

    @cached_property.cached_property
    def key_set(self):
        return set(self.d.keys())

    @cached_property.cached_property
    def encoded(self):
        _ = ason.encode(self.d)
        return _

    def _read_mtime(self, d):
        if d is None: return 0
        if '_last_modified' not in d: return 0
        return d['_last_modified'].timestamp()

    def graph_string(self):
        return bson.json_util.dumps(self.encoded, indent=2)

    def would_touch(self, mc):
        return False

    async def write_binary(self, b):
        assert isinstance(b, bytes)
        await self.write_contents(b)

    async def write_json(self, b):
        assert not asyncio.iscoroutine(b)
        await self.write_contents(b)

    async def write_string(self, b):
        assert isinstance(b, str)
        await self.write_contents(b)

    async def read_json(self):
        return await self.read_contents()

    async def read_string(self):
        s = await self.read_contents()
        if isinstance(s, bytes):
            s = s.decode()
        assert isinstance(s, str)
        return s

    async def read_binary(self):
        b = await self.read_contents()
        if not isinstance(b, bytes):
            print(f'{self!r} should be bytes but is {type(b)}')
            if input('delete?') == 'Y':
                await self.delete()
            raise TypeError(f'{self!r} is not bytes')
        return b

    @cached_property.cached_property
    def _id(self):
        d = pymake.client.client.find_one(self.encoded)

        #self._mtime = self._read_mtime(d)

        if d is None:
            res = pymake.client.client.insert_one(self.encoded)
            return res.inserted_id

        return str(d["_id"])

class ReqDoc0(ReqDocBase):
    """
    use mongodb to store pickled binary data
    """

    def __init__(self, d, **kwargs):
        super().__init__(d, **kwargs)
        logger.debug(repr(self))

        if d["type"] in [
            "node 0", 
            "node 1", 
            "node 20", 
            "node 90"]:
            raise Exception(f'invalid type for ReqDoc0: {d["type"]}')

    def __repr__(self):
        if 'type' not in self.d:
            print(self.d)
            breakpoint()
        d = {"type":self.d["type"]}
        return f'{self.__class__.__name__}(id={self._id}, {d!r})'

    def print_long(self):
        print(f'id: {self._id}')
        s = bson.json_util.dumps(self.encoded)
        print(s)
        pprint.pprint(self.encoded)
        #pprint.pprint(self.get_encoded())


    @cached_property.cached_property
    def _id(self):
        d = pymake.client.client.find_one(self.encoded)

        self._mtime = self._read_mtime(d)

        if d is None:
            res = pymake.client.client.insert_one(self.encoded)
            return res.inserted_id

        return str(d["_id"])

    def get_doc(self):
        d = pymake.req.client.find_one(self.encoded)
        return d

    def delete(self):
        logger.warning(crayons.yellow(f"deleteing {self!r}"))
        #res = pymake.client.client._coll.update_one(self.encoded, {'$unset': {'_last_modified': 1}})
        #if res.modified_count != 1:
        #    raise Exception(f"document: {self.d!r}. modified count should be 1 but is {res.modified_count}")

    async def output_exists(self):
        d = pymake.client.client.find_one(self.encoded)
        if d is None: return False
        b = bool('_last_modified' in d)
        
        if b:
            # look for FakePickle object

            s = d["_contents"] #self.read_contents()
            try:
                o = pickle.loads(s)
            except Exception as e:
                #logger.warning(f"pickle load error: {e!r}")
                pass
            else:
                if isinstance(o, FakePickle):
                    if not pymake.fakepickle.fake_pickle_archive.contains(o):
                        return False

        return b

    async def output_mtime(self):

        if hasattr(self, '_mtime'):
            logger.debug(crayons.blue('USING SAVED MTIME'))
            return self._mtime

        d = pymake.req.client.find_one(self.encoded)

        self._mtime = self._read_mtime(d)

        return self._mtime

    async def read(self):
        return await self.read_contents()

    async def read_contents(self):
        assert await self.output_exists()
        d = pymake.client.client.find_one(self.encoded)
        #if "_contents" not in d:
        #    breakpoint()
        return d["_contents"]

    def write_contents(self, b):
        # make sure is compatible
        #bson.json_util.dumps(b)
        t = pymake.client.client.update_one(self.encoded, {'$set': {'_contents': b}})
        self._mtime = t.timestamp()

class ReqDoc1(ReqDocBase):
    """
    using the doc registry
    """
    def __init__(self, d, **kwargs):
        super().__init__(d, **kwargs)
        logger.debug(repr(self))

        if "_id" in d: raise Exception('shouldnt contain "_id". {d!r}')

    def __eq__(self, other):
        #assert isinstance(self.d, dict)
        if not isinstance(other, ReqDoc1): return False
        #return self.d == other.d
        return self.encoded == other.encoded

    def __encode__(self):
        return {'/ReqDoc1': {'args': [ason.encode(self.d)]}}

    async def output_exists(self):
        return await pymake.doc_registry.registry.exists(self.encoded)
    
    async def output_mtime(self):
        return await pymake.doc_registry.registry.read_mtime(self.encoded)

    async def delete(self):
        await pymake.doc_registry.registry.delete(self.encoded)

    async def write_pickle(self, o):
        #logger.info("write pickle")
        await pymake.doc_registry.registry.write(self.encoded, o)

    async def write_contents(self, b):
        logger.debug(f"{self!r} write_contents")
        assert not asyncio.iscoroutine(b)
        await pymake.doc_registry.registry.write(self.encoded, b)

    def read_contents(self, mc=None):
        return pymake.doc_registry.registry.read(self.encoded)

    async def read_pickle(self, mc=None):
        o = await pymake.doc_registry.registry.read(self.encoded)
        assert not asyncio.iscoroutine(o)
        return o

    async def write(self, b):

        if isinstance(b, (str, dict, list)):
            await pymake.doc_registry.registry.write(self.encoded, b)
            return
        
        # attempt unpickle
        try:
            o = pickle.loads(b)
        except AttributeError as e:
            raise pymake.util.PickleError(repr(e))
        #except pickle.UnpicklingError as e:
        #    raise pymake.util.PickleError(repr(e))
        except Exception as e:
            logger.warning(crayons.yellow(repr(e)))
            o = b

        await pymake.doc_registry.registry.write(self.encoded, o)

class ReqDoc2(ReqDocBase):
   
    @classmethod
    async def new(cls, *args, **kwargs):
        o = cls(*args, **kwargs)
        await o.ainit()
        return o

    def __init__(self, d, **kwargs):
        super().__init__(d, **kwargs)
        self.kwargs = kwargs
        #logger.info(repr(self))

        if d["type"] in [
            #"node 1", 
            #"node 20", 
            "node 90"]: raise Exception()

    async def ainit(self):

        self.req1 = ReqDoc1(self.d, **self.kwargs)
        
        b1 = await self.req1.output_exists()

        if not b1:
            logger.debug(f"{self.req1!r} does not exist")

            self.req0 = ReqDoc0(self.d, **self.kwargs)

            b0 = await self.req0.output_exists()

            if b0:
                try:
                    b = await self.req0.read()
                    await self.req1.write(b)
                except pymake.util.PickleError as e:
                    await self.req0.delete()
                    raise

                assert await self.req1.output_exists()
        else:
            logger.debug(f"{self.req1!r} does exist")

    def __encode__(self):
       return {'/ReqDoc2': {'args': [ason.encode(self.d)]}}

    async def output_exists(self):
        if await self.req1.output_exists():
           return True

        if await self.req0.output_exists():
           try:
               self.req1.write(self.req0.read())
               assert self.req1.output_exists()
           except AttributeError as e:
               self.req0.delete()
           return True

        return False

    async def output_mtime(self):
        if await self.req1.output_exists():
           return await self.req1.output_mtime()

        if await self.req0.output_exists():
           return await self.req0.output_mtime()

    async def write_contents(self, b):
        #self.req0.write_contents(b)
        await self.req1.write_contents(b)

    async def write_pickle(self, o):
        #self.req0.write_pickle(o)
        await self.req1.write_pickle(o)

    async def read_pickle(self, mc=None):
        if await self.req1.output_exists():
           o = await self.req1.read_pickle(mc)
           assert not asyncio.iscoroutine(o)
           return o

        if await self.req0.output_exists():
           o = await self.req0.read_pickle(mc)
           await self.req1.write_pickle(o)
           assert not asyncio.iscoroutine(o)
           return o

    async def read_contents(self):
        assert await self.req1.output_exists()
        return await self.req1.read_contents()

        if await self.req1.output_exists():
           return await self.req1.read_contents()

        if await self.req0.output_exists():
           return await self.req1.read_contents()
        raise Exception()






