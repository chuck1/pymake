import contextlib
import datetime
import functools
import inspect
import pickle
import re
import os
import logging
import traceback
import pymongo
import bson

import pymake
from cached_property import cached_property
from mybuiltins import *
from mybuiltins import ason
from .exceptions import *
from .util import *
from .result import *
from .file_index import *

logger = logging.getLogger(__name__)

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
        return self.coll.find_one(q)

    def insert_one(self, q):
        return self.coll.insert_one(q)

    def update_one(self, q, u):
        if '$set' not in u:
            u['$set'] = {}

        d = self.coll.find_one(q)

        if d is None:
            res = self.coll.insert_one(q)
            _id = res.inserted_id
        else:
            _id = d["_id"]

        logger.info(crayons.green(f'write binary: {_id}'))

        u['$set']['_last_modified'] = datetime.datetime.now()
        
        self.coll.update_one({"_id": _id}, u)

client = Client()

def touch(fname, times=None):
    with open(fname, 'a'):
        os.utime(fname, times)

class Req:
    def output_exists(self):
        return None

    def output_mtime(self):
        return None

    def print_long(self):
        print(repr(self))

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
                self.print_long()
                print('exists', self.output_exists())
                print('mtime ', self.output_mtime())
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
        self.write_binary(pickle.dumps(o))

class ReqFake(Req):
    def __init__(self, fn):
        self.fn = fn

    async def make(self, makefile, mc, ancestor):
        return ResultNoBuild('is fake')

    def __repr__(self):
        return f'<{self.__class__.__name__} fn={self.fn!r}>'

class ReqFile(Req):
    """
    simple file requirement

    :param fn: relative path to file
    """
    def __init__(self, fn):
        assert isinstance(fn, str)
        self.fn = fn

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

    def load_text(self):
        try:
            with open(self.fn, 'r') as f:
                return f.read()
        except:
            logger.error(f'error loading: {self!r} {self.fn!r}')
            raise

    def load_object(self):
        try:
            with open(self.fn, 'rb') as f:
                return pickle.load(f)
        except:
            logger.error(crayons.red(f'error loading: {self!r} {self.fn!r}'))
            raise

    def load_pickle(self):
        try:
            with open(self.fn, 'rb') as f:
                return pickle.load(f)
        except:
            logger.error(f'error loading: {self.fn!r}')
            raise

    def load_json(self):
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

    def write_object(self, o):
        pymake.makedirs(os.path.dirname(self.fn))

        with open(self.fn, 'wb') as f:
            pickle.dump(o, f)

    def write_binary(self, b):
        pymake.makedirs(os.path.dirname(self.fn))
        with open(self.fn, 'wb') as f:
            f.write(b)

    def graph_string(self):
        return self.fn

    def read_pickle(self):
        with open(self.fn, 'rb') as f:
            try:
                return pickle.load(f)
            except Exception as e:
                logger.info('error unpickling', req)
                logger.info(e)
                raise

class ReqFileDescriptor(ReqFile):
    def __init__(self, d):
        self.d = d

        self.fn = manager.get_filename(self.d)

    def __repr__(self):
        #return f'{self.__class__.__name__}({self.d})'
        return f'{self.__class__.__name__}({{"type":{self.d["type"]}}})'

    def graph_string(self):
        return json.dumps(self.d, indent=2)

    def print_long(self):
        print(f'file: {self.fn}')
        pprint.pprint(self.d)

class ReqDoc(Req):
    def __init__(self, d):
        """
        d - bson-serializable object
        """
        self.d = d

    def __encode__(self):
        return {'/ReqDoc': {'args': [ason.encode(self.d)]}}

    def __repr__(self):
        return f'{self.__class__.__name__}({{"type":{self.d["type"]}}})'

    def get_encoded(self):
        _ = ason.encode(self.d)
        return _

    def print_long(self):
        print(f'id: {self.get_id()}')
        pprint.pprint(self.get_encoded())

    def get_id(self):
        d = client.find_one(self.get_encoded())

        if d is None:
            res = client.insert_one(self.get_encoded())
            return res.inserted_id

        return str(d["_id"])

    def graph_string(self):
        return json.dumps(self.d, indent=2)

    def output_exists(self):
        d = client.find_one(self.get_encoded())
        if d is None: return False
        return bool('_last_modified' in d)

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

    def write_contents(self, b):
        bson.json_util.dumps(b)
        client.update_one(self.get_encoded(), {'$set': {'_contents': b}})

    def read_contents(self):
        return client.find_one(self.get_encoded())["_contents"]

    def read_json(self):
        return self.read_contents()

    def read_pickle(self):
        try:
            return pickle.loads(self.read_contents())
        except:
            logger.error(f'error loading: {self!r}')
            raise


