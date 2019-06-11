import asyncio
import copy
import logging
import pickle
import pprint

import myhash
import cached_property
import crayons
import bson
import bson.json_util
from mybuiltins import *
from mybuiltins import hash1
import jelly
import pymake.req
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

        if not isinstance(d, pymake.desc.Desc):
            if isinstance(d, dict):
                assert 'type' not in d
                d = pymake.desc.Desc(**d)
            else:
                raise Exception(f'expected dict or Desc, not {type(d)} {d!r}')

        self.d = copy.deepcopy(d)

        self.build = build

    def copy(self, kw2):
        kw1 = copy.deepcopy(self.d._kwargs)
        kw1.update(kw2)
        d = self.d.__class__(**kw1)
        ret = self.__class__(d)

        ret.requirements_0 = list(self.requirements_0)
        ret.requirements_1 = list(self.requirements_1)

        return ret

    def __deepcopy__(self, memo):
        #print(f"{self.__class__.__name__} deepcopy")
        return self
        #return self.__class__(copy.deepcopy(self.d, memo))

    def _print(self):
        self.d._print()

    async def get_rule(self, mc):
        return await mc.makefile.find_one(mc, self)

    @property
    def type_(self):
        return self.d.type_

    def __repr__(self):
        
        d = {"type_": self.type_}
        
        for k in ("id", "condition", "coil"):
            if k in self.d._kwargs:
                d[k] = getattr(self.d, k)

        #up_to_date_0={self.up_to_date_0}, up_to_date_1={self.up_to_date_1}, 

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
        #ret = set(self.d._kwargs.keys())
        ret = set(self.d.encoded().keys())
        return ret

    @cached_property.cached_property
    def encoded(self):
        assert isinstance(self.d, pymake.desc.Desc)

        ret = self.d.encoded()

        # check
        #if __debug__:
        #    bson.json_util.dumps(ret)

        return ret

    def _read_mtime(self, d):
        if d is None: return 0
        if '_last_modified' not in d: return 0
        return d['_last_modified'].timestamp()

    def graph_string(self):
        return self.d.type_
        #return bson.json_util.dumps(self.encoded, indent=2)

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

    async def read_json(self, mc):
        ret = await self.read_contents(mc)
        assert not asyncio.iscoroutine(ret)
        return ret

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
            #if input('delete?') == 'Y':
            #    await self.delete()
            raise TypeError(f'{self!r} {b!r} is not bytes')
        return b

    @cached_property.cached_property
    def hash(self):
        hasher = myhash.Hasher(depth_stop=4)
        hasher(self.encoded)
        return hasher.hash()

    async def _get_id(self, mc):

        # try hash
   
        USE_HASH = False
            
        h = self.hash

        if USE_HASH:

            c = pymake.client.client.find({"hash": h})
            docs = await c.to_list(2)
    
            if len(docs) == 1:
                #logger.info(crayons.green("found id by hash"))
                return docs[0]["_id"]
    
            elif len(docs) > 1:
    
                raise Exception()
    
        return await mc.registry.get_id(self.encoded, h)



class ReqDoc1(ReqDocBase):
    """
    using the doc registry
    """
    def __init__(self, d, **kwargs):
        super().__init__(d, **kwargs)
        logger.debug(repr(self))

        if "_id" in self.d: raise Exception('shouldnt contain "_id". {d!r}')

    def __eq__(self, other):

        if not isinstance(other, ReqDoc1): return False

        for k0 in self.d.keys():

            assert hasattr(self.d, k0)

            if not hasattr(other.d, k0):
                return False

            if getattr(self.d, k0) != getattr(other.d, k0):
                return False


        if self.d.type_ != other.d.type_:
            return False

        return self.encoded == other.encoded

    async def output_exists(self):
        return await pymake.doc_registry.registry.exists(await self._id(), self.encoded)
    
    async def output_mtime(self):
        return await pymake.doc_registry.registry.read_mtime(await self._id(), self.encoded)

    async def delete(self):
        await pymake.doc_registry.registry.delete(await self._id(), self.encoded)

    async def write_pickle(self, o):
        await pymake.doc_registry.registry.write(await self._id(), self.encoded, o)

    async def write_contents(self, b):
        logger.debug(f"{self!r} write_contents")
        assert not asyncio.iscoroutine(b)
        await pymake.doc_registry.registry.write(await self._id(), self.encoded, b)

    async def read_contents(self, mc):
        ret = await mc.registry.read(self)#await self._id(), self.encoded)
        assert not asyncio.iscoroutine(ret)
        return ret

    async def read_pickle(self, mc=None):
        o = await pymake.doc_registry.registry.read(await self._id(), self.encoded)
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
        except Exception as e:
            logger.warning(crayons.yellow(repr(e)))
            o = b

        await pymake.doc_registry.registry.write(self.encoded, o)


class ReqDoc2:pass

