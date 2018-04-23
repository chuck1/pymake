__version__ = '0.2'

import asyncio
import functools
import inspect
import json
import os
import re
import sys
import logging
import pickle
import time
import traceback
import subprocess

from mybuiltins import *
from cached_property import cached_property

import pymake
from .pat import *
from .req import *
from .exceptions import *
from .util import *
from .result import *

logger = logging.getLogger(__name__)

def dict_get(d, k, de):
    if not k in d:
        d[k] = de
    return d[k]

class Rule_utilities:

    def write_object(self, file_out, o):
        pymake.makedirs(os.path.dirname(file_out))

        with open(file_out, 'wb') as f:
            pickle.dump(o, f)

    def write_json(self, d):
        pymake.makedirs(os.path.dirname(self.f_out))

        s = json.dumps(d, indent=4, sort_keys=True)

        with open(self.f_out, 'w') as f:
            f.write(s)

class _Rule(Rule_utilities):
    """
    Base class for rules.
    You must derive from this class and overwrite the following member functions:

    - ``f_in``
    - ``build``
    """
    def __init__(self):
        """
        """
        self.__rules = None
        self.up_to_date = False

    def build_requirements(self, loop, makecall, f):
        raise Exception(repr(self.__class__))
        yield

    async def build(self, makecall, _, f_in):
        """
        This function must be defined by a derived class.

        :param MakeCall makecall: makecall object
        :param _: DO NOT USE
        :param f_in: a list of ...
        """
        raise NotImplementedError()

    def _gen_rules(self, makecall):
        return
        yield

    def _rules(self, makecall=None):
        if self.__rules is None:
            self.__rules = list()
            for r in self._gen_rules(makecall):
                yield r
                self.__rules.append(r)
        else:
            yield from self.__rules
 
    def print_dep(self, makecall, indent):
        try:
            for f in self.f_in(makecall):
                makecall.makefile.print_dep(f, indent)
        except Exception as e:
            print("error in print_dep of {}".format(repr(self)))
            print(e)

    def output_exists(self):
        return False

    def output_mtime(self):
        return None

    async def make_ancestors(self, loop, makecall, test):

        makecall2 = makecall.copy()
        makecall2.args['test'] = test

        def func2(req):
            loop2 = asyncio.new_event_loop()

            return loop2.run_until_complete(makecall2.make(loop2, req, self.req_out))

        def func(req):
            if req is None:
                raise Exception("None in f_in {}".format(self))

            #f = functools.partial(makecall2.make, loop, req, self.req_out)

            if False:
                future = loop.run_in_executor(None, func2, req)
            else:
                future = loop.create_task(makecall2.make(loop, req, self.req_out))

            #r = makecall2.make(req, self.req_out)

            #if not isinstance(r, Result):
            #    print(req)
            #    print(r)
            #    raise Exception()
            
            #return req

            return future, req

        l = [t async for t in self.build_requirements(loop, makecall, func)]

        if not l: return

        try:
            futures, reqs = zip(*l)
        except:
            breakpoint()
            raise

        #done, pending = yield from asyncio.wait(futures)
        done, pending = await asyncio.wait(futures)

        for f in done:
            e = f.exception()
            if e:
                raise e

        #breakpoint()

        for req in reqs:
            yield req

    async def get_requirements(self, loop, makecall):
        
        def func(req): return req
        
        async for t in self.build_requirements(loop, makecall, func):
            yield t

    async def check(self, loop, makecall, test=False):
        
        f_in = [f async for f in self.make_ancestors(loop, makecall, test)]

        if makecall.args.get('force', False):
            return True, 'forced'

        #if not bool(f_in):
        #    raise Exception()
        
        b = self.output_exists()
        if not b:
            return True, "output does not exist"

        mtime = self.output_mtime()
        
        for f in f_in:
            if isinstance(f, Rule):
                continue
            
            if not isinstance(f, Req):
                breakpoint()
                raise Exception('{} f_in should return generator of Req objects, not {}'.format(repr(self), type(f)))

            if mtime is None:
                return True, '{} does not define mtime'.format(self.__class__.__name__)
            else:
                b = f.output_exists()
                if b:
                    mtime_in = f.output_mtime()

                    if mtime_in is None:
                        return True, 'input file {} does not define mtime'.format(repr(self))
                
                    if mtime_in > mtime:
                        return True, 'mtime of {} is greater'.format(f)
        
        return False, None

    async def _make(self, loop, makecall, req):

        if self.up_to_date: 
            raise Exception()
            return
        
        if req:
            if req.would_touch(makecall):
                should_build, f = self.check(makecall, test=True)
                if should_build:
                    req.touch_maybe(makecall)
                    return ResultBuild()
                else:
                    return ResultNoBuild()
            
        should_build, f = await self.check(loop, makecall, test=makecall.args.get('test', False))
        
        try:
            f_in = [t async for t in self.get_requirements(loop, makecall)]
        except Exception as e:
            print(self)
            traceback.print_exc()
            raise e

        if should_build:
            if makecall.args.get('test', False):
                #print(crayons.blue('build {} because {}'.format(repr(self), f)))
                logger.debug('build {} because {}'.format(repr(self), f))
                return ResultTestBuild()
            else:
                #blue('build {} because {}'.format(repr(self), f))
                try:
                    self._makecall = makecall
                    ret = await self._build(loop, makecall, f_in)
                except Exception as e:
                    logger.error(crayons.red('error building {}: {}'.format(repr(self), repr(e))))
                    raise

                if ret is None:
                    return ResultBuild()
                elif ret != 0:
                    raise BuildError(str(self) + ' return code ' + str(ret))
        else:
            if makecall.args.get('test', False):
                #print('DONT build',repr(self))
                return ResultTestNoBuild()
            else:
                return ResultNoBuild()

        self.up_to_date = True
    
    def write_text(self, filename, s):
        # it appears that this functionality is actually not useful as currently
        # implemented. revisit later

        #if check_existing_binary_data(filename, s.encode()):
        if True:
            pymake.os0.makedirs(os.path.dirname(filename))
            with open(filename, 'w') as f:
                f.write(s)
        else:
            print('binary data unchanged. do not write.')

    def write_pickle(self, o):
        b = pickle.dumps(o)
        self.write_binary(b)

    def write_binary(self, b):
        # it appears that this functionality is actually not useful as currently
        # implemented. revisit later

        #if check_existing_binary_data(filename, b):
        if True:
            pymake.makedirs(os.path.dirname(self.f_out))
            with open(self.f_out, 'wb') as f:
                f.write(b)
        else:
            print('binary data unchanged. do not write.')

    def rules(self, makecall):
        yield self
        for r in self._rules(makecall):
            yield from r.rules(makecall)

    async def _build(self, loop, makecall, *args):
        logger.info(crayons.yellow(f'Build {self!r}', bold=True))
        await self.build(loop, makecall, *args)

class Rule(_Rule):
    """
    a simple file-based rule

    :param str f_out: the filename that this rule builds
    """
    def __init__(self, f_out):
        _Rule.__init__(self)
        self.f_out = f_out
    
    def __repr__(self):
        return "<{}.{}>".format(
                self.__class__.__module__,
                self.__class__.__name__)

    def output_exists(self):
        return os.path.exists(self.f_out)
   
    def output_mtime(self):
        return os.path.getmtime(self.f_out)

    def test(self, req):
        """
        :param req: a requirement object

        determine if this rule builds ``req``
        """
        if not isinstance(req, ReqFile): return None
        if self.f_out == req.fn:
            return self
        return None

"""
a rule to which we can pass a static list of files for f_out and f_in
"""
class RuleStatic(_Rule):
    def __init__(self, static_f_out, static_f_in, func):
        
        self.static_f_in = static_f_in
        self.static_f_out = static_f_out

        self.build = func
        
    def f_out(self):
        return self.static_f_out

    def f_in(self, mc):
        yield self.static_f_in

class RuleRegex(_Rule):
    """
    A rule whose output filename is defined by a regex.
    You must derive from this class and define the follwing class attributes:
    
    - ``pat_out``

    :param str target: target
    :param list groups: regular expression groups
    """

    @classmethod
    def test(cls, req):
        if not isinstance(req, ReqFile): return None
        
        logger.debug('{} {}'.format(cls.pat_out, req.fn))

        if callable(cls.pat_out):
            pat = cls.pat_out()
        else:
            pat = cls.pat_out
        
        if pat is None: return

        #print(pat, req.fn)

        try:
            m = pat.match(req.fn)
        except Exception as e:
            print(crayons.red(e))
            print(crayons.red(repr(req)))
            print(crayons.red(repr(req.fn)))
            print(crayons.red(repr(pat)))
            raise

        if m is None: return None
        return cls(req.fn, m.groups())

    def output_exists(self):
        return os.path.exists(self.f_out)

    def output_mtime(self):
        return os.path.getmtime(self.f_out)

    def __init__(self, f_out, groups):
        self.f_out = f_out
        self.groups = groups

        _Rule.__init__(self)

    def __repr__(self):
        return '{}{}'.format(self.__class__.__name__, (self.f_out, self.groups))

        
class RuleRegexSimple(RuleRegex):
    async def build(self, makecall, _, f_in):
        print(crayons.yellow("Build " + repr(self),'yellow',bold=True))

        pymake.makedirs(os.path.dirname(self.f_out))
        
        if not f_in:
            raise Exception(f'{self.__class__} f_in is empty')

        if f_in[0].fn[-3:] == ".py":
            cmd = [sys.executable, f_in[0].fn, self.f_out] + [a.fn for a in f_in[1:]]
        else:
            prog = f_in[0].fn
            out = os.path.abspath(self.f_out)
            cmd = [prog, out] + [os.path.abspath(a.fn) for a in f_in[1:]]

        print(crayons.yellow("cmd = {}".format(' '.join(cmd)),'yellow',bold=True))

        subprocess.run(cmd, check=True)

class RuleRegexSimple2(RuleRegex):
    async def build(self, loop, makecall, f_in):
        #logger.info(crayons.yellow("Build " + repr(self),'yellow',bold=True))
        #for f in f_in:
        #    logger.info(crayons.yellow(f'  {f!r}'))

        pymake.makedirs(os.path.dirname(self.f_out))

        assert f_in[0].fn[-3:] == ".py"
        
        av = [f_in[0].fn, self.f_out] + [a.fn for a in f_in[1:]]
        
        s = f_in[0].fn[:-3].replace('/','.')
        m = __import__(s, fromlist=['main'])

        await m.main(loop, makecall, av)

class RuleFileDescriptor(Rule):
    """
    a rule that defines a file descriptor pattern
    a file descriptor pattern is a dict in which the attributes are regular values or patterns that can be used to match regular values
    """

    @classmethod
    def test(cls, req):
        if not isinstance(req, ReqFileDescriptor): return None
 
       
        # a - this descriptor
        # b - req descriptor

        pat = cls.descriptor_pattern()

        a = pat
        b = dict(req.d)
       
        if 'type' in pat:
            logger.debug(crayons.blue(f'pat type={pat["type"]}'))

        logger.debug(f'pat={pat}')
        logger.debug(f'dsc={req.d}')

        set_a = set(a.keys())
        set_b = set(b.keys())
        
        a_and_b = set_a & set_b

        just_pat = set_a - set_b
        just_dsc = set_b - set_a

        for k in a_and_b:
            if isinstance(a[k], Pat):
                if not a[k].match(b[k]):
                    logger.debug(f'{k!r} does not match pattern')
                    return None
            else:
                if not (a[k] == b[k]):
                    logger.debug(f'{k!r} differs')
                    return None

        # attributes in the pattern but not in the descriptor must be nullable
        for k in just_pat:
            if not isinstance(pat[k], PatNullable):
                logger.debug(f'{k!r} is in pattern but not descriptor and is not nullable')
                return None
            else:
                b[k] = pat[k].default
        
        # attributes in the descriptor but not in the pattern must be null
        for k in just_dsc:
            if b[k] is not None:
                logger.debug(f'{k!r} is in descriptor but not in pattern and is not None')
                return None

        return cls(req.fn, b)

    def __init__(self, f_out, descriptor):
        super(RuleFileDescriptor, self).__init__(f_out)
        self.f_out = f_out
        self.descriptor = descriptor

    def graph_string(self):
        return json.dumps(self.descriptor, indent=4)

    def __repr__(self):
        return f'<{self.__class__} filename={self.f_out!r}>'

class RuleFileDescriptorSimple(RuleFileDescriptor):

    async def build(self, loop, makecall, f_in):
        logger.info(crayons.yellow("Build " + repr(self),'yellow',bold=True))
        for f in f_in:
            logger.info(crayons.yellow(f'  {f!r}'))

        pymake.makedirs(os.path.dirname(self.f_out))

        assert f_in[0].fn[-3:] == ".py"
        
        av = [f_in[0].fn, self.f_out] + [a.fn for a in f_in[1:]]
        
        s = f_in[0].fn[:-3].replace('/','.')
        m = __import__(s, fromlist=['main'])
        
        print(s)
        print(m)
        print(m.main)

        await m.main(loop, makecall, av)


