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
logger_check = logging.getLogger(__name__+"-check")

THREADED = False
USE_TASKS = False

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

    async def write_json(self, d):
        await self.req.write_json(d)

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
        self.req_out = None

    async def requirements_0(self, makecall, func):
        logger.warning(f'{self.__class__!r} does not define requirements_0')
        async for _ in self.build_requirements(makecall, func): yield _

    async def requirements_1(self, makecall, func):
        logger.warning(f'{self.__class__!r} does not define requirements_1')
        return
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

    def output_mtime(self):
        return None


    def __requirements_threaded_(mc, req):
            
        loop = asyncio.new_event_loop()

        coro = mc.make(req, ancestor=self.req_out)

        ret = loop.run_until_complete(coro)

        assert isinstance(ret, Result)

        return req

    async def __requirements_func(self, req, makecall=None, test=None, threaded=None):

        if req is None:
            return

        if not isinstance(req, pymake.req.Req):
            raise TypeError(f'expected Req not {type(req)}')

        makecall2 = makecall.copy(test=test, force=False)

        if makecall2.args.force:
            breakpoint()
            makecall.copy(test=test, force=False)
            raise Exception()

        if threaded:
            makecall2.thread_depth = makecall.thread_depth + 1
            task = loop.run_in_executor(None, functools.partial(threaded_, makecall2, req))
            return task
        else:
            
            if USE_TASKS:

                # use task

                async def _f():
                    await makecall2.make(req, ancestor=self.req_out)
                    return req

                task = asyncio.ensure_future(_f())
                assert isinstance(task, asyncio.Task)
                return task

            else:
                r = await makecall2.make(req, ancestor=self.req_out)
                assert isinstance(r, Result)
                return req


    async def __requirements(self, makecall, test, requirements_function, 
            threaded=False, 
            req_requirements=[],
            ):
        """

        :param req_requirements: list of reqs that was stored in the self.req object

        requirements that can be skipped if requirements_0 are up_to_date and the req
        has stored that it is up to date
        """

        loop = asyncio.get_event_loop()

        # the build_requirements function yields the results of calling func on Req objects
        # and func returns those Req objects

        func = functools.partial(self.__requirements_func, test=test, makecall=makecall, threaded=threaded)

        async def _chain():

            for req in req_requirements:

                ret = await func(req)

                if USE_TASKS:
                    if not isinstance(ret, asyncio.Task):
                        raise Exception()

                yield ret

            async for req in requirements_function(makecall, func):
                
                if USE_TASKS:
                    if not isinstance(req, asyncio.Task):
                        raise Exception()

                yield req

        if USE_TASKS:
            tasks = [_ async for _ in _chain()]

            if tasks:
                done, pending = await asyncio.wait(tasks)
            
            for task in tasks:
                e = task.exception()
                if e:
                    raise e

            lst = [task.result() for task in tasks]

            for e in lst:
                if not isinstance(e, pymake.req.Req):
                    raise Exception("task should return Req")
        else:
            lst = [_ async for _ in _chain()]


        for req in lst:
            
            logger.debug(repr(req))

            if threaded:
                if not isinstance(req, asyncio.Future): raise TypeError(f'expected Task got {req!r}')
                yield req
            else:

                if __debug__ and asyncio.iscoroutine(req): 
                    raise Exception(f'{self!r}')

                if req is not None: 
                    if USE_TASKS:
                        pass
                    else:
                        if not isinstance(req, Req):
                            raise Exception(f'{self!r} should return Req objects, not {req!r}')

                logger_check.debug(repr(req))

                yield req

    async def __check_requirements_get_reqs(self, mc, test, requirements_function, req_requirements):
    
        lst = [r async for r in self.__requirements(mc, test, requirements_function, req_requirements=req_requirements)]

        return lst


    async def __check_requirements(
            self, 
            makecall, 
            requirements_function, 
            req=None, 
            test=False,
            req_requirements=[],
            ):
        """
        :param req_requirements: requirements stored in the req object
        """

        threaded = THREADED
        if makecall.thread_depth > 2:
            threaded = False

        if threaded:
            tasks = [r async for r in self.__requirements(makecall, test, requirements_function, threaded)]
            if tasks:
                logger.info(f'len(tasks) = {len(tasks)}')
                for task in tasks:
                    logger.info(f'  {task}')
                done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_EXCEPTION)
                if pending: raise Exception()
                for task in tasks:
                    if task.exception():
                        raise task.exception()
                reqs = [task.result() for task in done]
            else:
                reqs = list()
        else:
            reqs = await self.__check_requirements_get_reqs(makecall, test, requirements_function, req_requirements)


        if makecall.args.force: return True, 'forced', reqs

        b = await self.req.output_exists()

        if not b: 
            return True, "output does not exist", reqs

        mtime = await self.output_mtime()
        
        if mtime is None:
            return True, '{} does not define mtime'.format(self.__class__.__name__), reqs

    

        for f in reqs:
            logger.debug(f'check {f!r}')

            # allow None in reqs
            if f is None: continue

            b = await f.output_exists()
            if b:
                mtime_in = await f.output_mtime()

                if mtime_in is None:
                    return True, 'input file {} does not define mtime'.format(repr(self)), reqs
            
                if mtime_in > mtime:
                    return True, 'mtime of {} is greater'.format(f), reqs
        
        return False, 'up to date', reqs

    async def __check(self, makecall, req=None, test=False):


        if req.up_to_date_0:
            if not makecall.args.force:
                logger.debug(crayons.green('up to date'))
                return False, "up_to_date_0", None

        logger.debug('check requirements 0')

        b_0, s_0, reqs_0 = await self.__check_requirements(makecall, self.requirements_0, req=req, test=test,
                req_requirements=self.req.requirements_0,
                )

        req.create_triggers_0(makecall.makefile, reqs_0)

        if not b_0:
            if req.up_to_date_1: 
                if not makecall.args.force:
                    logger.debug(crayons.green('up to date'))
                    return False, "up_to_date_1", None
        
        logger.debug('check requirements 1')

        b_1, s_1, reqs_1 = await self.__check_requirements(makecall, self.requirements_1, req=req, test=test,
                req_requirements=self.req.requirements_1,
                )

        req.create_triggers_1(makecall.makefile, reqs_1)

        if b_0:
            logger.debug(f'{self.__class__!r}. b_0 = True. s_0 = {s_0!r}')
            for r in reqs_0:
                logger.debug(f'  {r!r}')

        logger.debug(f'checking requirements_1 for {req!r}. b_0 = {b_0}. up_to_date_1 = {req.up_to_date_1}')
        for r in reqs_1:
            logger.debug(f'  {r!r}')


        reqs = reqs_0 + reqs_1

        if makecall.args.force:
            logger.warning(crayons.yellow("forced"))
            return True, "forced", reqs

        # these reqs will become triggers for this req.
        # next time we try to build this req, ...

        if b_0:
            logger.info(f'build {req!r} because {s_0!r}')
            return True, s_0, reqs

        if b_1:
            logger.info(f'build {req!r} because {s_1!r}')
            return True, s_1, reqs

        return False, "up_to_date", None

    async def _make_touch(self, makecall, req):

        should_build, f = self.rule__check(makecall, req=req, test=True)
        
        if should_build:
            req.touch_maybe(makecall)
            return ResultBuild()
        else:
            return ResultNoBuild()

    async def _make(self, makecall, req):
        logger.debug(f'test = {makecall.args.test}')
        logger.debug(f'make {req!r}')

        if makecall.args.force:
            logger.warning(crayons.yellow("forced"))

        # touch
        # TODO not sure if compatible with req_cache
        #if req and req.would_touch(makecall):
        #    return self._make_touch(makecall, req)

            
        should_build, f, reqs = await self.__check(makecall, req=req, test=makecall.args.test)


        if makecall.args.test:
            return self._make_test(makecall, req, should_build)


        if should_build:

            try:
                ret = await self._build(makecall, req, None, reqs)
            except Exception as e:
                logger.error(crayons.red('error building {}: {}'.format(repr(self), repr(e))))
                raise

            self.req.set_up_to_date_0(True)
            self.req.set_up_to_date_1(True)

            return ResultBuild()

        else:
            # we know that either req.up_to_date is already True or req.maybe_create_triggers was called
            # and therefore it is safe to set this as up_to_date
            self.req.set_up_to_date_0(True)
            self.req.set_up_to_date_1(True)

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

    def rules(self, makecall):
        yield self
        for r in self._rules(makecall):
            yield from r.rules(makecall)

    async def _build(self, makecall, req, *args):

        logger.info(crayons.yellow(f'Build {req}', bold=True))

        if False:
            for line in lines(functools.partial(self.print_long, self.req)):
                logger.debug(crayons.yellow(line, bold=True))
               
        await self.build(makecall, *args)
        
        # call callbacks
        for req1 in req._on_build:
            req1.set_up_to_date_1(False)



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
            raise Exception()
        
        l = []

        for k, v in cls.descriptor_pattern().items():
            if isinstance(v, pymake.pat.PatNullable):
                continue

            l.append(k)

        l = set(l)

        if 'type_' in l: l.remove('type_')

        return l

    @classmethod
    async def test(cls, mc, req):
        if not isinstance(req, pymake.req.req_doc.ReqDocBase): return None
       
        # a - this descriptor
        # b - req descriptor

       
        a = cls.descriptor_pattern()

        if not 'type_' in a:
            raise Exception(repr(cls))

        b = req.d.encoded()
        #b = dict(req.d._kwargs)

        type_a = a['type_']
        type_b = req.type_

        logger.debug(crayons.blue(f'pat type={type_a}'))

        if type_a != type_b:
            logger.debug(f'type a ({type_a!r}) != type b ({type_b!r})')
            return

        
        ks0 = cls.descriptor_keys_required()
        ks1 = req.key_set


        if bool(ks0 - ks1):
            if type_a == type_b:
                logger.debug(f'ks0 = {ks0}')
                logger.debug(f'ks1 = {ks1}')
            logger.debug(f'(ks0 - ks1) is not empty: {ks0 - ks1!r}. cls = {cls}')
            return


       

        logger.debug(f'pat={a}')
        logger.debug(f'dsc={req.d}')

        try:
            b1 = await mc.decoder.adecode(b)
        except:
            logger.error(crayons.red("failed to decode:"))
            logger.error(str(b)[:1000])
            raise

        if 'type_' in a:
            del a['type_']
        if 'type_' in b1:
            del b1['type_']

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


async def _copy(src, dst):
    async with src.open('r')as f0:
        s = f0.read()

    async with dst.open('w') as f1:
        f1.write(s)

async def _copy_binary(src, dst):
    async with src.open('rb')as f0:
        s = f0.read()

    async with dst.open('wb') as f1:
        f1.write(s)

class RuleDocCopy(RuleDoc):
    async def build(self, mc, _, _1):
        await _copy(await self.req_0(mc), self.req)

class RuleDocCopyBinary(RuleDoc):
    async def build(self, mc, _, _1):
        await _copy_binary(await self.req0(mc), self.req)

class RuleDocCopyObject(RuleDoc):
    async def build(self, mc, _, reqs):
        o = await (await self.req_0(mc)).read_pickle()
        await self.req.write_pickle(o)








