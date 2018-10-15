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

class ReqFuture:
    # the result of calling func(req)
    # a thread is created and a future is returned

    def __init__(self, req, fut):
        self.req = req
        self.fut = fut

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
        self.req.write_json(d)

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
        self.req_out = None

    async def requirements_0(self, makecall, func):
        raise Exception(repr(self.__class__))
        yield

    async def requirements_1(self, makecall, func):
        raise Exception(repr(self.__class__))
        yield

    async def build_requirements(self, makecall, func):
        raise Exception(repr(self.__class__))
        yield

    def build(self, makecall, _, f_in):
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
            logging.error("error in print_dep of {}".format(repr(self)))
            logging.error(e)

    def msg(self, s):
        logger.warning(crayons.green(f'{s}: {self!r}'))
        return
        try:
            logger.info(crayons.green(f'{s}: {self.req.get_id()}'))
        except Exception as e:
            try:
                logger.info(crayons.green(f'{s}: {self.req!r}'))
            except:
                logger.info(crayons.green(f'{s}: {self!s}'))


    def print_long(self, req):
        print(type(self))
        req.print_long()

    def output_exists(self):
        return False

    def output_mtime(self):
        return None

    async def make_ancestors(self, makecall, test):

        async def func(req):
            assert req is not None
            assert isinstance(req, pymake.req.Req)


            makecall2 = makecall.copy(test=test, force=False)

            r = await makecall2.make(req, ancestor=self.req_out)


            assert isinstance(r, Result)


            return req

        logger.debug(crayons.red(self))

        # the build_requirements function yields the results of calling func on Req objects
        # and func returns those Req objects

        async for req in self.build_requirements(makecall, func):
                        
            if asyncio.iscoroutine(req): raise Exception(f'{self!r}')
            
            yield req

    async def rule__check(self, makecall, req=None, test=False):
        
        reqs = [r async for r in self.make_ancestors(makecall, test)]

        req.maybe_create_triggers(makecall.makefile, reqs)

        if makecall.args.force:
            return True, 'forced', reqs

        b = self.output_exists()
        if not b:
            return True, "output does not exist", reqs

        mtime = self.output_mtime()
        
        if mtime is None:
            return True, '{} does not define mtime'.format(self.__class__.__name__), reqs


        for f in reqs:
            if isinstance(f, Rule):
                continue
            
            if not isinstance(f, Req):
                raise Exception(
                    f'{self!r} reqs should return generator of Req objects, not {f!r}')

            logger.debug(f'check {f!r}')

            b = f.output_exists()
            if b:
                mtime_in = f.output_mtime()

                if mtime_in is None:
                    return True, 'input file {} does not define mtime'.format(repr(self)), reqs
            
                if mtime_in > mtime:
                    return True, 'mtime of {} is greater'.format(f), reqs
        

        return False, 'up to date', reqs

    async def _make(self, makecall, req):
        logger.debug(f'test = {makecall.args.test}')
        logger.debug(f'make {req!r}')

        test = makecall.args.test

        if self.up_to_date: 
            raise Exception()
            return
        
        if req:
            if req.would_touch(makecall):
                should_build, f = self.rule__check(makecall, req=req, test=True)
                if should_build:
                    req.touch_maybe(makecall)
                    return ResultBuild()
                else:
                    return ResultNoBuild()
            
        should_build, f, reqs = await self.rule__check(makecall, req=req, test=test)


        if test:
            return self._make_test(makecall, req, should_build)


        if should_build:

            try:
                ret = await self._build(makecall, req, None, reqs)
            except Exception as e:
                logger.error(crayons.red('error building {}: {}'.format(repr(self), repr(e))))
                raise

            self.req.set_up_to_date(True)
            return ResultBuild()

        else:

            self.req.set_up_to_date(True)
            return ResultNoBuild()

    def _make_test(self, makecall, req, should_build):

         if should_build:

            logging.error(crayons.blue(f'Build because {f}:'))
            print_lines(
                    lambda s: logging.error(crayons.blue(s)),
                    functools.partial(self.print_long, req),)

            return ResultTestBuild()
         else:
            return ResultTestNoBuild()
  
    def write_text(self, filename, s):
        # it appears that this functionality is actually not useful as currently
        # implemented. revisit later

        #if check_existing_binary_data(filename, s.encode()):
        if True:
            pymake.os0.makedirs(os.path.dirname(filename))
            with open(filename, 'w') as f:
                f.write(s)
        else:
            logging.info('binary data unchanged. do not write.')

    def DEPwrite_pickle(self, o):
        b = pickle.dumps(o)
        self.write_binary(b)

    def DEPwrite_binary(self, b):
        # it appears that this functionality is actually not useful as currently
        # implemented. revisit later

        #if check_existing_binary_data(filename, b):
        if True:
            pymake.makedirs(os.path.dirname(self.req.fn))
            with open(self.req.fn, 'wb') as f:
                f.write(b)
        else:
            logging.info('binary data unchanged. do not write.')

    def rules(self, makecall):
        yield self
        for r in self._rules(makecall):
            yield from r.rules(makecall)

    async def _build(self, makecall, req, *args):
        for line in lines(
                lambda: print(f'Build force={makecall.args.force}'),
                functools.partial(self.print_long, self.req)):
            logger.info(crayons.yellow(line, bold=True))
               
        await self.build(makecall, *args)
        
        # call callbacks
        for req1 in req._on_build:
            req1.set_up_to_date(False)



class Rule(_Rule):
    """
    a simple file-based rule

    :param str f_out: the filename that this rule builds
    """
    def __init__(self, req):
        super(Rule, self).__init__()
        self.req = req
    
    def __repr__(self):
        return "<{}.{}>".format(
                self.__class__.__module__,
                self.__class__.__name__)

    def output_exists(self):
        return self.req.output_exists()
   
    def output_mtime(self):
        return self.req.output_mtime()

    async def test(self, mc, req):
        """
        :param req: a requirement object

        determine if this rule builds ``req``
        """
        if not isinstance(req, ReqFile): return None
        if self.req.fn == req.fn:
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
    async def test(cls, mc, req):
        if not isinstance(req, ReqFile): return None
        
        #logger.debug('{} {}'.format(cls.pat_out, req.fn))

        if callable(cls.pat_out):
            pat = cls.pat_out()
        else:
            pat = cls.pat_out
        
        if pat is None: return

        try:
            m = pat.match(req.fn)
        except Exception as e:
            logging.error(crayons.red(e))
            logging.error(crayons.red(repr(req)))
            logging.error(crayons.red(repr(req.fn)))
            logging.error(crayons.red(repr(pat)))
            raise

        if m is None: return None
        return cls(req, m.groups())

    def output_exists(self):
        return os.path.exists(self.f_out)

    def output_mtime(self):
        return os.path.getmtime(self.f_out)

    def __init__(self, req, groups):
        self.req = req
        self.f_out = req.fn
        self.groups = groups

        _Rule.__init__(self)

    def __repr__(self):
        return '{}{}'.format(self.__class__.__name__, (self.f_out, self.groups))

        
class RuleRegexSimple(RuleRegex):
    async def build(self, makecall, _, f_in):

        pymake.makedirs(os.path.dirname(self.f_out))
        
        if not f_in:
            raise Exception(f'{self.__class__} f_in is empty')

        if f_in[0].fn[-3:] == ".py":
            cmd = [sys.executable, f_in[0].fn, self.f_out] + [a.fn for a in f_in[1:]]
        else:
            prog = f_in[0].fn
            out = os.path.abspath(self.f_out)
            cmd = [prog, out] + [os.path.abspath(a.fn) for a in f_in[1:]]

        logging.info(crayons.yellow("cmd = {}".format(' '.join(cmd)),'yellow',bold=True))

        subprocess.run(cmd, check=True)

class RuleRegexSimple2(RuleRegex):
    async def build(self, makecall, _, f_in):
        for f in f_in:
            logger.info(crayons.yellow(f'  {f!r}'))

        pymake.makedirs(os.path.dirname(self.f_out))

        assert f_in[0].fn[-3:] == ".py"
        
        av = [f_in[0].fn, self.f_out] + [a for a in f_in[1:]]
        
        s = f_in[0].fn[:-3].replace('/','.')
        m = __import__(s, fromlist=['main'])
        
        c = await m.main(makecall, av)

class RuleDoc(Rule):
    """
    a rule that defines a file descriptor pattern
    a file descriptor pattern is a dict in which the attributes are regular values or patterns that can be used to match regular values
    """

    @classmethod
    def descriptor_keys_required(cls):
        if hasattr(cls, "_keys_required"):
            return cls._keys_required
        
        l = []

        for k, v in cls.descriptor_pattern().items():
            if isinstance(v, pymake.pat.PatNullable):
                continue

            l.append(k)

        l = set(l)

        cls._keys_required = l

        return cls._keys_required

    @classmethod
    async def test(cls, mc, req):
        if not isinstance(req, pymake.req.req_doc.ReqDocBase): return None
       
        # a - this descriptor
        # b - req descriptor

        ks0 = cls.descriptor_keys_required()
        ks1 = req.key_set
        
        pat = cls.descriptor_pattern()

        if bool(ks0 - ks1):
            if pat['type'] == req.d['type']:
                logger.debug(f'ks0 = {ks0}')
                logger.debug(f'ks1 = {ks1}')
            return


        a = pat
        b = dict(req.d)
       
        if 'type' in pat:
            logger.debug(crayons.blue(f'pat type={pat["type"]}'))

        logger.debug(f'pat={pat}')
        logger.debug(f'dsc={req.d}')

        try:
            b1 = await mc.decoder.decode(b, mc.copy(force=False))
        except:
            logger.error(crayons.red("failed to decode:"))
            pprint.pprint(b)
            raise
    
        if not match_dict(a, b1): return

        return cls(req, b1)

    def __init__(self, req, descriptor):
        super(RuleDoc, self).__init__(req)
        self.descriptor = descriptor
        self.d = descriptor

    def graph_string(self):
        return json.dumps(self.descriptor, indent=4)

    def __repr__(self):
        return f'<{self.__class__} desc={self.descriptor!r} req={self.req!r}>'

class RuleDocSimple(RuleDoc):

    async def build(self, makecall, _, f_in):
        for f in f_in:
            logger.info(crayons.yellow(f'  {f!r}'))

        filename = self.req.fn

        pymake.makedirs(os.path.dirname(filename))

        assert f_in[0].fn[-3:] == ".py"
        
        av = [f_in[0].fn, filename] + [a.fn for a in f_in[1:]]
        
        s = f_in[0].fn[:-3].replace('/','.')
        m = __import__(s, fromlist=['main'])
        
        await m.main(makecall, av)


def _copy(src, dst):
    with src.open('r')as f0:
        s = f0.read()

    with dst.open('w') as f1:
        f1.write(s)

def _copy_binary(src, dst):
    with src.open('rb')as f0:
        s = f0.read()

    with dst.open('wb') as f1:
        f1.write(s)

class RuleDocCopy(RuleDoc):
    async def build(self, makecall, _, requirements):
        _copy(requirements[0], self.req)

class RuleDocCopyBinary(RuleDoc):
    async def build(self, makecall, _, requirements):
        _copy_binary(requirements[0], self.req)

class RuleDocCopyObject(RuleDoc):
    async def build(self, mc, _, reqs):
        self.req.write_pickle(reqs[0].read_pickle())



