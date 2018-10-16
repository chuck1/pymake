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
    def __init__(self, d, build=True):
        """
        d     - bson-serializable object. once initialized, MUST NOT CHANGE. has already been decoded
        build - flag is this should be built or just read
        """
        super().__init__()

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

    def write_binary(self, b):
        self.write_contents(b)

    def write_json(self, b):
        self.write_contents(b)

    def write_string(self, b):
        assert isinstance(b, str)
        self.write_contents(b)

    def read_json(self):
        return self.read_contents()

    def read_string(self):
        s = self.read_contents()
        if isinstance(s, bytes):
            s = s.decode()
        assert isinstance(s, str)
        return s

    def read_binary(self):
        b = self.read_contents()
        assert isinstance(b, bytes)
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
        res = pymake.req.client._coll.update_one(self.encoded, {'$unset': {'_last_modified': 1}})
        if res.modified_count != 1:
            raise Exception(f"document: {self.d!r}. modified count should be 1 but is {res.modified_count}")

    def output_exists(self):
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

    def output_mtime(self):

        if hasattr(self, '_mtime'):
            logger.debug(crayons.blue('USING SAVED MTIME'))
            return self._mtime

        d = pymake.req.client.find_one(self.encoded)

        self._mtime = self._read_mtime(d)

        return self._mtime

    def read_pickle(self, mc=None):
 
        #logger.info(f"read pickle {self.encoded}")
   
        return super().read_pickle(mc)

    def read(self):
        return self.read_contents()

    def read_contents(self):
        assert self.output_exists()
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

    def __eq__(self, other):
        assert isinstance(self.d, dict)
        if not isinstance(other, ReqDoc1): return False
        return self.d == other.d

    def __encode__(self):
        return {'/ReqDoc1': {'args': [ason.encode(self.d)]}}

    def output_exists(self):
        return pymake.doc_registry.registry.exists(self.encoded)
    
    def output_mtime(self):
        return pymake.doc_registry.registry.read_mtime(self.encoded)

    def delete(self):
        pymake.doc_registry.registry.delete(self.encoded)

    def write_pickle(self, o):
        #logger.info("write pickle")
        pymake.doc_registry.registry.write(self.encoded, o)

    def write_contents(self, b):
        logger.info(f"{self!r} write_contents")
        pymake.doc_registry.registry.write(self.encoded, b)

    def read_contents(self, mc=None):
        return pymake.doc_registry.registry.read(self.encoded)

    def read_pickle(self, mc=None):
        return pymake.doc_registry.registry.read(self.encoded)

    def write(self, b):

        if isinstance(b, (str, dict, list)):
            pymake.doc_registry.registry.write(self.encoded, b)
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

        pymake.doc_registry.registry.write(self.encoded, o)

class ReqDoc2(ReqDocBase):
    
    def __init__(self, d, **kwargs):
        super().__init__(d, **kwargs)
        #logger.info(repr(self))

        if d["type"] in [
            #"node 1", 
            #"node 20", 
            "node 90"]: raise Exception()

        self.req1 = ReqDoc1(d, **kwargs)

        if not self.req1.output_exists():
            logger.debug(f"{self.req1!r} does not exist")

            self.req0 = ReqDoc0(d, **kwargs)

            if self.req0.output_exists():
                try:
                    b = self.req0.read()
                    self.req1.write(b)
                except pymake.util.PickleError as e:
                    self.req0.delete()
                    raise

                assert self.req1.output_exists()
        else:
            logger.debug(f"{self.req1!r} does exist")

    def __encode__(self):
       return {'/ReqDoc2': {'args': [ason.encode(self.d)]}}

    def output_exists(self):
        if self.req1.output_exists():
           return True

        if self.req0.output_exists():
           try:
               self.req1.write(self.req0.read())
               assert self.req1.output_exists()
           except AttributeError as e:
               self.req0.delete()
           return True

        return False

    def output_mtime(self):
        if self.req1.output_exists():
           return self.req1.output_mtime()

        if self.req0.output_exists():
           return self.req0.output_mtime()

    def write_contents(self, b):
        self.req1.write_contents(b)

    def write_pickle(self, o):
        self.req1.write_pickle(o)

    def read_pickle(self, mc=None):
        if self.req1.output_exists():
           return self.req1.read_pickle(mc)

        if self.req0.output_exists():
           o = self.req0.read_pickle(mc)
           self.req1.write_pickle(o)
           return o

    def read_contents(self):
        assert self.req1.output_exists()
        return self.req1.read_contents()

        if self.req1.output_exists():
           return self.req1.read_contents()

        if self.req0.output_exists():
           return self.req1.read_contents()
        raise Exception()






