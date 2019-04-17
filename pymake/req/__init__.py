import asyncio
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
import json
import pprint

import pymongo
import bson
import cached_property

import jelly
from mybuiltins import *

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

class Req(jelly.Serializable):

    build = True

    def __init__(self, *args, require_rule=False, **kwargs):
        self._type = None
    
        self.require_rule = require_rule

        # volatile
        # bool
        # requirements_0 are all up_to_date
        # cannot be pickled 
        self._up_to_date_0 = False

        # bool
        # requirements_1 are all up_to_date
        # can be pickled
        self._up_to_date_1 = False

        self.__on_build = []

    def _print(self):
        print(repr(self))

    @property
    def _on_build(self):
        if not hasattr(self, "__on_build"): self.__on_build = []
        return self.__on_build

    def __jellygetstate__(self, encoder):
        keys = ('d',)
        dct = dict(((k, self.__dict__[k]) for k in keys))
        return dct

    def __getstate__(self):
        state = dict(self.__dict__)
        
        # the nature of up_to_date_0 is that it 

        if "_up_to_date_0" in state:
            del state["_up_to_date_0"]

        return state

    def __setstate__(self, state):
        #if "_Req__up_to_date_0" not in state:
        #    state["_Req__up_to_date_0"] = False
        #if "_Req__up_to_date_1" not in state:
        #    state["_Req__up_to_date_1"] = False

        self.__dict__ = dict(state)
        
        # validate
        if not hasattr(self, 'require_rule'): 
            logger.error('no attr \'require_rule\'')
            self.require_rule = False
        self.require_rule

    def __jellysetstate__(self, state):
        #if "_Req__up_to_date_0" not in state:
        #    state["_Req__up_to_date_0"] = False
        #if "_Req__up_to_date_1" not in state:
        #    state["_Req__up_to_date_1"] = False

        self.__dict__ = dict(state)
        
        # validate
        if not hasattr(self, 'require_rule'): 
            logger.debug('no attr \'require_rule\'')
            self.require_rule = False

        self.require_rule
    
    @property
    def up_to_date_0(self):
        if not hasattr(self, '_up_to_date_0'): self._up_to_date_0 = False
        value = self._up_to_date_0
        logger.debug(f'{id(self)!r} {self!r} {value}')
        assert hasattr(self, '_up_to_date_0')
        return value

    @property
    def up_to_date_1(self):
        if not hasattr(self, '_up_to_date_1'): self._up_to_date_1 = False
        value = self._up_to_date_1
        logger.debug(f'{id(self)!r} {self!r} {value}')
        assert hasattr(self, '_up_to_date_1')
        return value

    def set_up_to_date_0(self, value):
        assert isinstance(value, bool)
        logger.debug(f'{id(self)!r} {self!r} {value}')
        self._up_to_date_0 = value
        assert hasattr(self, '_up_to_date_0')

    def set_up_to_date_1(self, value):
        assert isinstance(value, bool)
        logger.debug(f'{id(self)!r} {self!r} {value}')
        self._up_to_date_1 = value
        assert hasattr(self, '_up_to_date_1')

    def create_triggers_0(self, makefile, reqs):
        # do not skip this is self.reqs is already set.
        # we might be updating self.reqs because something in requirements_0 changed

        # TODO should we also be searching for instances of this in the _on_build
        # lists of all cached reqs? so that if this no longer depends on something the
        # reference will be removed?

        self.reqs_0 = reqs

        for req in reqs:

            # make sure req is in cache
            #req = makefile.cache_get(req)

            if self not in req._on_build:
                req._on_build.append(self)

    def create_triggers_1(self, makefile, reqs):

        self.reqs_1 = reqs
        
        for req in reqs:
            if req is None:
                continue
            if self not in req._on_build:
                req._on_build.append(self)

    async def output_exists(self):
        return None

    async def output_mtime(self):
        return None

    def print_long(self):
        print(repr(self))

    def open(self, mode):
        return OpenContext(self, mode)

    async def get_rule(self, mc):
        logger.debug(f"{self}")

        rules = await mc.makefile.rules_sorted(mc, self)

        if len(rules) == 0:
            logger.debug(f'no rules to make {self!r}')
            b = await self.output_exists()
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
            if self.require_rule:
                logger.warning(crayons.yellow("no rule and require_rule is True"))
            else:
                if await self.output_exists():
                    return pymake.result.ResultNoRuleFileExists()

            # error
            for line in lines(self.print_long): logger.error(crayons.red(line))
            raise Exception(f"no rule to make {self!r}")

        assert pymake.util._isinstance(rule, pymake.rules._Rule)

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

    async def write_pickle(self, o):
       
        self._stored = o
        
        try:
            logger_pickle.info(f"pickle dump {o!r}")
            b = pickle.dumps(o)
        except Exception as e:
            logger.warning(crayons.yellow(repr(e)))
            # use FakePickle object
            
            logger.debug(f'write fake pickle')
            for line in lines(functools.partial(pprint.pprint, self.d)): logger.debug(line)

            p = fake_pickle_archive.write(o)
            logger_pickle.info(f"pickle dump {p!r}")
            b = pickle.dumps(p)

        await self.write_binary(b)
        
    async def read_pickle(self, mc=None):

        if hasattr(self, '_stored'):
            logger.warning(crayons.yellow(f'HAS STORED! {self!r}'))

        #if mc is not None:
        #    await mc.make(self)

        b = await self.read_binary()

        try:
            o = pickle.loads(b)
            logger_pickle.debug(f"pickle load")
            for line in lines(self.print_long): logger_pickle.debug(line)
        except Exception as e:
            logger.error(crayons.red('pickle error'))
            logger.error(crayons.red(repr(e)))
            logger.error(crayons.red(f'delete {self!r}'))
            breakpoint()
            #logger.error(b)
            
            #self.delete()
 
            raise

            #if mc is None:
            #    raise

            #await mc.make(self)

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
        assert not asyncio.iscoroutine(o)
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

    async def copy_binary(self, filename):
        b = await self.read_binary()
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

    def __jellygetstate__(self, encoder):
        keys = ('fn',)
        dct = dict(((k, self.__dict__[k]) for k in keys))
        return dct

    def __eq__(self, other):
        if not isinstance(other, ReqFile):
            return False

        if self.fn == other.fn:
            return True

        #logger.info(f'{self.fn!r} != {other.fn!r}')
        return False

    async def output_exists(self):
        """
        check if the file exists
        """
        return os.path.exists(self.fn)

    async def output_mtime(self):
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

    async def write_binary(self, b):
        assert isinstance(b, bytes)
        pymake.util.makedirs(os.path.dirname(self.fn))
        with open(self.fn, 'wb') as f:
            f.write(b)

    def graph_string(self):
        return self.fn

    async def read_string(self):
        with open(self.fn, 'r') as f:
            return f.read()

    async def read_binary(self):
        with open(self.fn, 'rb') as f:
            ret = f.read()
            return ret

    def delete(self):
        os.remove(self.fn)

    async def read_pickle(self, mc=None):
        if self.fn.endswith('.csv'):
            raise Exception()
        await super().read_pickle(mc=mc)

class ReqFake(Req):
    def __init__(self, fn=None):
        super().__init__()
        self.fn = fn

    async def _make(self, mc, ancestor):
        return ResultNoBuild('is fake')

    async def output_exists(self):
        return None

    async def output_mtime(self):
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

    async def __aenter__(self):

        if self.mode == 'w':
            self.f = FileW(io.StringIO())
        elif self.mode == 'wb':
            self.f = FileW(io.BytesIO())
        elif self.mode == 'r':
            self.f = FileR(io.StringIO(await self.req.read_string()))
        elif self.mode == 'rb':
            self.f = FileR(io.BytesIO(await self.req.read_binary()))
        
        return self.f

    async def __aexit__(self, exc_type, _2, _3):
        if exc_type is not None:
            return

        if self.mode == 'w':
            s = self.f.buf.getvalue()
            await self.req.write_string(s)
        elif self.mode == 'wb':
            s = self.f.buf.getvalue()
            await self.req.write_binary(s)

class ReqTemp(Req):
   
    def __init__(self, b=None):
        super().__init__()
        self.b = b

    async def _make(self, mc, ancestor):
        return ResultNoBuild('is temp')

    async def output_exists(self):
        return hasattr(self, 'b')

    async def read_pickle(self):
        assert not asyncio.iscoroutine(self.b)
        return self.b

    async def read_binary(self):
        return self.b

    async def read_string(self):
        assert isinstance(self.b, str)
        return self.b

    async def write_pickle(self, b):
        assert not asyncio.iscoroutine(b)
        self.b = b

    async def write_binary(self, b):
        assert isinstance(b, bytes)
        self.b = b

    async def write_string(self, b):
        assert isinstance(b, str)
        self.b = b



