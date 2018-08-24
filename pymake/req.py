import contextlib
import datetime
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

import pymake
import cached_property
from mybuiltins import *
from mybuiltins import ason
from .exceptions import *
from .util import *
from .result import *
from .fakepickle import *

logger = logging.getLogger(__name__)
logger_pickle = logging.getLogger(__name__ + '-pickle')
logger_mongo  = logging.getLogger(__name__ + '-mongo')

class Client:
    def __init__(self):
        client = pymongo.MongoClient()
        db = client.coiltest
        self._coll = db.test

    def exists(self, q):
        c = self.coll.find(q).limit(1)
        try:
            next(c)
            return True
        except StopIteration:
            return False

    def find_one(self, q):
        logger_mongo.debug(f"find_one {q!r}")
        return self._coll.find_one(q)

    def insert_one(self, q):
        try:
            return self._coll.insert_one(q)
        except:
            logger.error(crayons.red('error in mongo insert'))
            pprint.pprint(q)
            raise

    def update_one(self, q, u):
        if '$set' not in u:
            u['$set'] = {}

        d = self._coll.find_one(q)

        if d is None:
            res = self._coll.insert_one(q)
            _id = res.inserted_id
        else:
            _id = d["_id"]

        logger.debug(crayons.green(f'write binary: {_id}'))

        t = datetime.datetime.now()

        u['$set']['_last_modified'] = t
        
        self._coll.update_one({"_id": _id}, u)
        
        return t

client = Client()

def touch(fname, times=None):
    with open(fname, 'a'):
        os.utime(fname, times)

class Req:

    build = True

    def output_exists(self):
        return None

    def output_mtime(self):
        return None

    def print_long(self):
        print(repr(self))

    def open(self, mode):
        return OpenContext(self, mode)

    async def make(self, mc, ancestor):
        logger.debug(repr(self))
        logger.debug(f'makecall args: {mc.args!r}')

        mc.add_edge(ancestor, self)

        #if not mc.args['test']:
        if True:
            if self in mc.makefile._cache_req:
                logger.debug('in cache')
                return ResultNoBuild('in cache')
       
        mc.makefile._cache_req.append(self)
    
        rules = await mc.makefile.rules_sorted(mc, self)

        if len(rules) == 0:
            logger.debug('no rules')
            b = self.output_exists()
            if b:
                return ResultNoRuleFileExists()
            else:
                print_lines(logger.warning, self.print_long)
                #logging.debug('exists', self.output_exists())
                #logging.debug('mtime ', self.output_mtime())
                raise NoTargetError("no rules to make {}".format(repr(self)))
       
        rule = rules[0]

        #if self.touch_maybe(mc): return

        #mc.add_edge(ancestor, rule)

        #for rule in rules:

        try:
            ret = await rule._make(mc, self)
        except NoTargetError as e:
            print('while building', repr(self))
            print(' ',e)
            raise
        
        return ret

    def would_touch(self, mc):
        if not os.path.exists(self.fn):
            return False

        for touch_str in mc.args.get('touch', []):
            if touch_str:
                #print(crayons.yellow(f'touch: {touch_str}'))
                pat = re.compile(touch_str)
                m = pat.match(self.fn)
                if m:
                    return True
        return False

    def touch_maybe(self, mc):
        if self.would_touch(mc):
            print(crayons.yellow(f'touch: {self.fn}'))
            touch(self.fn)
            return True
        return False

    def write_pickle(self, o):
       
        self._stored = o
        
        try:
            logger_pickle.info(f"pickle dump {o!r}")
            b = pickle.dumps(o)
        except Exception as e:
            logger.warning(crayons.yellow(repr(e)))
            # use FakePickle object
            
            logger.debug(f'write fake pickle')
            print_lines(logger.debug, lambda: pprint.pprint(self.d))

            p = fake_pickle_archive.write(o)
            logger_pickle.info(f"pickle dump {p!r}")
            b = pickle.dumps(p)

        self.write_binary(b)
        
    async def read_pickle(self, mc=None):

        if hasattr(self, '_stored'):
            logger.warning(crayons.yellow(f'HAS STORED! {self!r}'))

        if mc is not None:
            await mc.make(self)

        b = self.read_binary()

        try:
            o = pickle.loads(b)
            logger_pickle.debug(f"pickle load")
            print_lines(logger_pickle.debug, self.print_long)
        except Exception as e:
            logger.error(crayons.red('pickle error'))
            logger.error(crayons.red(repr(e)))
            logger.error(crayons.red(f'delete {self!r}'))
            #logger.error(b)
            
            await self.delete()

            if mc is None:
                raise

            await mc.make(self)

            b = self.read_binary()
            o = pickle.loads(b)
            logger_pickle.info(f"pickle load {o!r}")

        if isinstance(o, FakePickle):
            if fake_pickle_archive.contains(o):
                o = fake_pickle_archive.read(o)
            else:
                await self.delete()
                print_lines(logger.error, lambda: pprint.pprint(self.d))
                print_lines(logger.error, lambda: pprint.pprint(fake_pickle_archive._map))
                logger.error(f'{o.i} {(o.i in fake_pickle_archive._map)}')
                raise Exception('got FakePickle that is not in the archive')

        self._stored = o
        return o

    def read_csv(self):
        import csv

        #ith open('eggs.csv', newline='') as f:
        with self.open('r') as f:

            reader = csv.reader(
                    f, 
                    delimiter=',', 
                    #quotechar='|',
                    )
            for row in reader:
                yield row

class ReqFile(Req):
    """
    simple file requirement

    :param fn: relative path to file
    """
    def __init__(self, fn):
        assert isinstance(fn, str)
        self.fn = fn

    def __encode__(self):
        return {'/ReqFile': {'args': [self.fn]}}

    def __eq__(self, other):
        if not isinstance(other, self.__class__):
            return False

        return self.fn == other.fn

    def output_exists(self):
        """
        check if the file exists
        """
        return os.path.exists(self.fn)

    def output_mtime(self):
        """
        return the mtime of the file
        """
        return os.path.getmtime(self.fn)

    def __repr__(self):
        return 'pymake.ReqFile({})'.format(repr(self.fn))

    def read_text(self):
        try:
            with open(self.fn, 'r') as f:
                return f.read()
        except:
            logger.error(f'error loading: {self!r} {self.fn!r}')
            raise

    def read_json(self):
        with open(self.fn, 'r') as f:
            s = f.read()
        try:
            return json.loads(s)
        except:
            logger.error(f'error loading: {self.fn!r}')
            breakpoint()
            raise

    def write_json(self, d):
        pymake.makedirs(os.path.dirname(self.fn))

        with open(self.fn, 'w') as f:
            f.write(json.dumps(d, indent=8, sort_keys=True))

    def write_text(self, s):
        pymake.makedirs(os.path.dirname(self.fn))

        with open(self.fn, 'w') as f:
            f.write(s)

    def write_binary(self, b):
        pymake.util.makedirs(os.path.dirname(self.fn))
        with open(self.fn, 'wb') as f:
            f.write(b)

    def graph_string(self):
        return self.fn

    def read_text(self):
        with open(self.fn, 'r') as f:
            return f.read()

    def read_binary(self):
        with open(self.fn, 'rb') as f:
            return f.read()

    async def delete(self):
        os.remove(self.fn)

class ReqFake(ReqFile):
    def __init__(self, fn):
        self.fn = fn

    async def make(self, mc, ancestor):
        return ResultNoBuild('is fake')

    def output_exists(self):
        return None

    def output_mtime(self):
        return None

    def __repr__(self):
        return f'<{self.__class__.__name__} fn={self.fn!r}>'

class FileW:
    def __init__(self, buf):
        self.buf = buf

    def write(self, b):
        self.buf.write(b)

class FileR:
    def __init__(self, buf):
        self.buf = buf

    def read(self, size=-1):
        return self.buf.read(size)

    def readline(self):
        return self.buf.readline()

    def readlines(self):
        return self.buf.readlines()

    def __iter__(self):
        return iter(self.buf)

class OpenContext:
    def __init__(self, req, mode):
        self.req = req
        self.mode = mode
        
        assert mode in ('w', 'wb', 'r', 'rb')

        if self.mode == 'w':
            self.f = FileW(io.StringIO())
        elif self.mode == 'wb':
            self.f = FileW(io.BytesIO())
        elif self.mode == 'r':
            self.f = FileR(io.StringIO(self.req.read_text()))
        elif self.mode == 'rb':
            self.f = FileR(io.BytesIO(self.req.read_binary()))
        
    def __enter__(self):
        return self.f

    def __exit__(self, exc_type, _2, _3):
        if exc_type is not None: return

        if self.mode == 'w':
            s = self.f.buf.getvalue()
            self.req.write_text(s)
        elif self.mode == 'wb':
            s = self.f.buf.getvalue()
            self.req.write_binary(s)


class ReqDoc(Req):
    def __init__(self, d, build=True):
        """
        d     - bson-serializable object. once initialized, MUST NOT CHANGE
        build - flag is this should be built or just read
        """

        if not isinstance(d, dict):
            raise Exception()

        assert 'type' in d
        self.d = d

        self.build = build

    def __encode__(self):
        return {'/ReqDoc': {'args': [ason.encode(self.d)]}}

    def __repr__(self):
        if 'type' not in self.d:
            print(self.d)
            breakpoint()
        return f'{self.__class__.__name__} id = {self._id} {{"type":{self.d["type"]!r}}}'

    @cached_property.cached_property
    def key_set(self):
        return set(self.d.keys())

    @cached_property.cached_property
    def encoded(self):
        _ = ason.encode(self.d)
        return _

    def print_long(self):
        print(f'id: {self._id}')
        s = bson.json_util.dumps(self.encoded)
        print(s)
        pprint.pprint(self.encoded)
        #pprint.pprint(self.get_encoded())

    def get_doc(self):
        d = client.find_one(self.encoded)
        return d

    @cached_property.cached_property
    def _id(self):
        d = client.find_one(self.encoded)

        self._mtime = self._read_mtime(d)

        if d is None:
            res = client.insert_one(self.encoded)
            return res.inserted_id

        return str(d["_id"])

    def _read_mtime(self, d):
        if d is None: return 0
        if '_last_modified' not in d: return 0
        return d['_last_modified'].timestamp()

    def graph_string(self):
        return bson.json_util.dumps(self.encoded, indent=2)

    async def delete(self):
        res = client._coll.update_one(self.encoded, {'$unset': {'_last_modified': 1}})
        if res.modified_count != 1:
            raise Exception(f"document: {self.d!r}. modified count should be 1 but is {res.modified_count}")

    def output_exists(self):
        d = client.find_one(self.encoded)
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
                    if not fake_pickle_archive.contains(o):
                        return False


        return b

    def would_touch(self, mc):
        return False

    def output_mtime(self):

        if hasattr(self, '_mtime'):
            logger.debug(crayons.blue('USING SAVED MTIME'))
            return self._mtime

        d = client.find_one(self.encoded)

        self._mtime = self._read_mtime(d)

        return self._mtime

    def write_binary(self, b):
        self.write_contents(b)

    def write_json(self, b):
        self.write_contents(b)

    def write_text(self, b):
        assert isinstance(b, str)
        self.write_contents(b)

    def write_contents(self, b):
        # make sure is compatible
        #bson.json_util.dumps(b)
        t = client.update_one(self.encoded, {'$set': {'_contents': b}})
        self._mtime = t.timestamp()

    def read_contents(self):
        assert self.output_exists()
        d = client.find_one(self.encoded)
        #if "_contents" not in d:
        #    breakpoint()
        return d["_contents"]

    def read_json(self):
        return self.read_contents()

    def read_text(self):
        s = self.read_contents()
        if isinstance(s, bytes):
            s = s.decode()
        assert isinstance(s, str)
        return s

    def read_binary(self):
        b = self.read_contents()
        assert isinstance(b, bytes)
        return b

class ReqTemp(Req):
    
    async def make(self, mc, ancestor):
        return ResultNoBuild('is temp')

    def output_exists(self):
        return hasattr(self, 'b')

    def write_pickle(self, b):
        self.b = b

    async def read_pickle(self):
        return self.b

    def write_binary(self, b):
        self.b = b

    def read_binary(self):
        return self.b

    def write_text(self, b):
        self.b = b

    def read_text(self):
        assert isinstance(self.b, str)
        return self.b


