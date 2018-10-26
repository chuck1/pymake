import contextlib
import datetime
import enum
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

import cached_property
from mybuiltins import *
from mybuiltins import ason

from pymake.exceptions import *
from pymake.util import *
from pymake.result import *
from pymake.fakepickle import *
import pymake

#import pymake.doc_registry

logger = logging.getLogger(__name__)
logger_pickle = logging.getLogger(__name__ + '-pickle')
logger_mongo  = logging.getLogger(__name__ + '-mongo')


def touch(fname, times=None):
    with open(fname, 'a'):
        os.utime(fname, times)

class Datatype(enum.Enum):
    STRING = 0
    BYTES = 1
    OBJECT = 2

class Req:

    build = True

    def __init__(self, *args, **kwargs):
        self._type = None
    
        # volatile
        self.__up_to_date = False
        self._on_build = []

    def __setstate__(self, state):
        if "_Req__up_to_date" not in state:
            state["_Req__up_to_date"] = False
        self.__dict__ = dict(state)
    
    @property
    def up_to_date(self):
        #if not hasattr(self, "reqs"):
        #    return False
        return self.__up_to_date

    def set_up_to_date(self, value):
        assert isinstance(value, bool)
        self.__up_to_date = value

    def maybe_create_triggers(self, makefile, reqs):
        if not hasattr(self, "reqs"):
            self.reqs = reqs

            for req in reqs:

                # make sure req is in cache
                #req = makefile.cache_get(req)

                req._on_build.append(self)

    def output_exists(self):
        return None

    def output_mtime(self):
        return None

    def print_long(self):
        print(repr(self))

    def open(self, mode):
        return OpenContext(self, mode)

    async def get_rule(self, mc):
        rules = await mc.makefile.rules_sorted(mc, self)

        if len(rules) == 0:
            logger.debug(f'no rules to make {self!r}')
            b = self.output_exists()
            if b:
                return
            else:
                for line in lines(self.print_long): logger.warning(line)
                #logging.debug('exists', self.output_exists())
                #logging.debug('mtime ', self.output_mtime())
                raise NoTargetError("no rules to make {}".format(repr(self)))
       
        return rules[0]

    async def _make(self, mc, ancestor):
        logger.debug(repr(self))
        logger.debug(f'makecall args: {mc.args!r}')

        if __debug__:
            mc.makefile.add_edge(ancestor, self)

        #if not mc.args['test']:
        if False:
            if self in mc.makefile._cache_req:
                logger.debug('in cache')
                return ResultNoBuild('in cache')
       
            mc.makefile._cache_req.append(self)

        rule = await self.get_rule(mc)

        if rule is None:
            if self.output_exists():
                return pymake.result.ResultNoRuleFileExists()
            else:
                for line in lines(self.print_long): logger.error(crayons.red(line))
                raise Exception(f"no rule to make {self!r}")

        assert pymake.util._isinstance(rule, pymake.rules._Rule)

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
        
    def read_pickle(self, mc=None):

        if hasattr(self, '_stored'):
            logger.warning(crayons.yellow(f'HAS STORED! {self!r}'))

        #if mc is not None:
        #    await mc.make(self)

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
            
            self.delete()
 
            raise

            #if mc is None:
            #    raise

            #await mc.make(self)

            #b = self.read_binary()
            #o = pickle.loads(b)
            #logger_pickle.info(f"pickle load {o!r}")

        if isinstance(o, FakePickle):
            if fake_pickle_archive.contains(o):
                o = fake_pickle_archive.read(o)
            else:
                self.delete()
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

    def copy_binary(self, filename):
        b = self.read_binary()
        pymake.utils.makedirs(os.path.dirname(filename))
        with open(filename, 'wb') as f:
            f.write(b)

class ReqFile(Req):
    """
    simple file requirement

    :param fn: relative path to file
    """
    def __init__(self, fn):
        super().__init__()
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
            
        return json.loads(s)

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

    def delete(self):
        os.remove(self.fn)

class ReqFake(Req):
    def __init__(self, fn=None):
        super().__init__()
        self.fn = fn

    async def _make(self, mc, ancestor):
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
            self.f = FileR(io.StringIO(self.req.read_string()))
        elif self.mode == 'rb':
            self.f = FileR(io.BytesIO(self.req.read_binary()))
        
    def __enter__(self):
        return self.f

    def __exit__(self, exc_type, _2, _3):
        if exc_type is not None: return

        if self.mode == 'w':
            s = self.f.buf.getvalue()
            self.req.write_string(s)
        elif self.mode == 'wb':
            s = self.f.buf.getvalue()
            self.req.write_binary(s)

class ReqTemp(Req):
    
    async def _make(self, mc, ancestor):
        return ResultNoBuild('is temp')

    def output_exists(self):
        return hasattr(self, 'b')

    def write_pickle(self, b):
        self.b = b

    def read_pickle(self):
        return self.b

    def read_binary(self):
        return self.b

    def read_string(self):
        assert isinstance(self.b, str)
        return self.b

    def write_binary(self, b):
        self.b = b

    def write_string(self, b):
        self.b = b

    def read_text(self):
        assert isinstance(self.b, str)
        return self.b


