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
from cached_property import cached_property
from mybuiltins import *
from mybuiltins import ason
from .exceptions import *
from .util import *
from .result import *

logger = logging.getLogger(__name__)

class FakePickleArchive:

    def __init__(self):
        self._next_id = int(time.time()*1000)

        self._map = {}

    def next_id(self):
        i = self._next_id
        self._next_id += 1
        return i

    def write(self, o):

        i = self.next_id()

        p = FakePickle(i)

        self._map[i] = o

        logger.debug(f'fake pickle archive write: {o} {i} {p}')

        return p

    def contains(self, p):
        assert isinstance(p, FakePickle)
        return (p.i in self._map)

    def read(self, p):
        assert isinstance(p, FakePickle)
        return self._map[p.i]

fake_pickle_archive = FakePickleArchive()

class FakePickle:
    def __init__(self, i):
        self.i = i

def clean_doc(d0):
    d1 = dict(d0)

    keys_to_delete = [k for k in d1.keys() if k.startswith("_")]
    
    for k in keys_to_delete:
        del d1[k]

    return d1


class Client:
    def __init__(self):
        client = pymongo.MongoClient()
        db = client.coiltest
        self.coll = db.test

    def exists(self, q):
        c = self.coll.find(q).limit(1)
        try:
            next(c)
            return True
        except StopIteration:
            return False

    def find_one(self, q):
        #logger.info(f"find_one {q!r}")
        return self.coll.find_one(q)

    def insert_one(self, q):
        try:
            return self.coll.insert_one(q)
        except:
            logger.error(crayons.red('error in mongo insert'))
            pprint.pprint(q)
            raise

    def update_one(self, q, u):
        if '$set' not in u:
            u['$set'] = {}

        d = self.coll.find_one(q)

        if d is None:
            res = self.coll.insert_one(q)
            _id = res.inserted_id
        else:
            _id = d["_id"]

        logger.debug(crayons.green(f'write binary: {_id}'))

        u['$set']['_last_modified'] = datetime.datetime.now()
        
        return self.coll.update_one({"_id": _id}, u)

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

    async def make(self, makefile, mc, ancestor):
        logger.debug(repr(self))

        mc.add_edge(ancestor, self)

        #if not mc.args['test']:
        if True:
            if self in makefile._cache_req:
                logger.debug('in cache')
                return ResultNoBuild('in cache')
       
        makefile._cache_req.append(self)
    
        rules = await makefile.rules_sorted(mc, self)

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
        
        try:
            b = pickle.dumps(o)
        except:
            # use FakePickle object
            
            logger.debug(f'write fake pickle')
            print_lines(logger.debug, lambda: pprint.pprint(self.d))

            p = fake_pickle_archive.write(o)
            b = pickle.dumps(p)

        self.write_binary(b)
        
    async def read_pickle(self, mc=None):

        if mc is not None:
            await mc.make(self)

        b = self.read_binary()

        try:
            o = pickle.loads(b)
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

        if isinstance(o, FakePickle):
            if fake_pickle_archive.contains(o):
                o = fake_pickle_archive.read(o)
            else:
                await self.delete()
                print_lines(logger.error, lambda: pprint.pprint(self.d))
                print_lines(logger.error, lambda: pprint.pprint(fake_pickle_archive._map))
                logger.error(f'{o.i} {(o.i in fake_pickle_archive._map)}')
                raise Exception('got FakePickle that is not in the archive')

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
        pymake.makedirs(os.path.dirname(self.fn))
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

    async def make(self, makefile, mc, ancestor):
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
        d     - bson-serializable object
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
        return f'{self.__class__.__name__} id = {self.get_id()} {{"type":{self.d["type"]!r}}}'

    def get_encoded(self):
        _ = ason.encode(self.d)
        return _

    def print_long(self):
        print(f'id: {self.get_id()}')
        s = bson.json_util.dumps(self.get_encoded())
        print(s)
        #pprint.pprint(self.get_encoded())

    def get_doc(self):
        d = client.find_one(self.get_encoded())
        return d

    def get_id(self):
        d = client.find_one(self.get_encoded())

        if d is None:
            res = client.insert_one(self.get_encoded())
            return res.inserted_id

        return str(d["_id"])

    def graph_string(self):
        return json.dumps(self.get_encoded(), indent=2)

    async def delete(self):
        res = client.coll.update_one(self.get_encoded(), {'$unset': {'_last_modified': 1}})
        if res.modified_count != 1:
            raise Exception(f"document: {self.d!r}. modified count should be 1 but is {res.modified_count}")

    def output_exists(self):
        d = client.find_one(self.get_encoded())
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
        t = client.find_one(self.get_encoded()).get("_last_modified", None)
        if t is None: return 0
        return t.timestamp()

    def write_binary(self, b):
        self.write_contents(b)

    def write_json(self, b):
        self.write_contents(b)

    def write_text(self, b):
        assert isinstance(b, str)
        self.write_contents(b)

    def write_contents(self, b):
        # make sure is compatible
        bson.json_util.dumps(b)

        client.update_one(self.get_encoded(), {'$set': {'_contents': b}})

    def read_contents(self):
        assert self.output_exists()
        d = client.find_one(self.get_encoded())
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
    
    def write_binary(self, b):
        self.b = b

    def read_binary(self):
        return self.b

    def write_text(self, b):
        self.b = b

    def read_text(self):
        assert isinstance(self.b, str)
        return self.b


