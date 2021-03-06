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

    async def build_requirements(self, makecall, f):
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

    def output_exists(self):
        return False

    def output_mtime(self):
        return None

    async def make_ancestors(self, makecall, test):

        makecall2 = makecall.copy(test=test, force=False)

        async def func(req):
            if req is None:
                raise Exception("None in f_in {}".format(self))

            r = await makecall2.make(req, ancestor=self.req_out)

            if not isinstance(r, Result):
                logging.error(req)
                logging.error(r)
                raise Exception()
            
            return req

        logger.debug(crayons.red(self))

        async for r in self.build_requirements(makecall2, func):
            #print(crayons.red(self))
            yield (await r)

    async def get_requirements(self, makecall):
        
        async def func(req): return req
        
        async for r in self.build_requirements(makecall, func):
            yield (await r)

    async def check(self, makecall, test=False):
        
        f_in = [r async for r in self.make_ancestors(makecall, test)]

        if makecall.args.get('force', False):
            return True, 'forced'

        if not bool(f_in):
            breakpoint()
            raise Exception()
        
        b = self.output_exists()
        if not b:
            return True, "output does not exist"

        mtime = self.output_mtime()
        
        for f in f_in:
            if isinstance(f, Rule):
                continue
            
            if not isinstance(f, Req):
                raise Exception(
                    f'{self!r} f_in should return generator of Req objects, not {f!r}')

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
        

        return False, 'up to date'

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

    async def _make(self, makecall, req):
        logger.debug(f'makecall. test = {makecall.args.get("test", False)}')

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
            
        should_build, f = await self.check(makecall, test=makecall.args.get('test', False))
        #self.msg(f'should build: {should_build!s:5}. {f}')
       
        try:
            f_in = [r async for r in self.get_requirements(makecall)]
        except Exception as e:
            logging.error(repr(self))
            traceback.print_exc()
            raise e

        if should_build:
            if makecall.args.get('test', False):

                logging.error(crayons.blue(f'Build because {f}:'))
                print_lines(
                        lambda s: logging.error(crayons.blue(s)),
                        functools.partial(self.print_long, req),)

                return ResultTestBuild()
            else:
                #blue('build {} because {}'.format(repr(self), f))
                try:
                    self._makecall = makecall
                    ret = await self._build(makecall, None, f_in)
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
            logging.info('binary data unchanged. do not write.')

    def write_pickle(self, o):
        b = pickle.dumps(o)
        self.write_binary(b)

    def write_binary(self, b):
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

    async def _build(self, makecall, *args):
        print_lines(
                lambda s: logger.info(crayons.yellow(s, bold=True)),
                lambda: print(f'Build force={makecall.args.get("force", False)}'),
                functools.partial(self.print_long, self.req))

        await self.build(makecall, *args)

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
        
        logger.debug('{} {}'.format(cls.pat_out, req.fn))

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
    async def test(cls, mc, req):
        if not isinstance(req, ReqDoc): return None
       
        # a - this descriptor
        # b - req descriptor

        pat = cls.descriptor_pattern()

        a = pat
        b = dict(req.d)
       
        #breakpoint()

        if 'type' in pat:
            logger.debug(crayons.blue(f'pat type={pat["type"]}'))

        logger.debug(f'pat={pat}')
        logger.debug(f'dsc={req.d}')

        set_a = set(a.keys())
        set_b = set(b.keys())
        
        a_and_b = set_a & set_b

        just_pat = set_a - set_b
        just_dsc = set_b - set_a
        
        b0 = False #"type" in a_and_b and a["type"] == b["type"]
        with context_if(functools.partial(logger_level_context, logger, logging.DEBUG), b0):

            for k in a_and_b:
                if isinstance(a[k], Pat):
                    if not a[k].match(b[k]):
                        logger.debug(f'{cls} {k!r} does not match pattern {a[k]!r} {b[k]!r}')
                        return None
    
                    if isinstance(a[k], PatNullable) and b[k] is None:
                        b[k] = a[k].default
                    
                else:
                    if not (a[k] == b[k]):
                        #logger.debug(f'{k!r} differs')
                        return None
    
            # attributes in the pattern but not in the descriptor must be nullable
            for k in just_pat:
                if not isinstance(pat[k], PatNullable):
                    logger.debug(f'{cls} {k!r} is in pattern but not descriptor and is not nullable')
                    return None
                else:
                    b[k] = pat[k].default
            
            # attributes in the descriptor but not in the pattern must be null
            for k in just_dsc:
                if b[k] is not None:
                    logger.debug(f'{cls} {k!r} is in descriptor but not in pattern and is not None')
                    return None
   
            #logger.debug(crayons.green(f'match {a} and {b}'))

        return cls(req, await mc.decoder.decode(b, mc.copy(force=False)))

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



