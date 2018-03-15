__version__ = '0.2'

import functools
import inspect
import os
import re
import sys
import logging
import pickle
import time
import traceback
import subprocess

from cached_property import cached_property

import pymake
from .req import *
from .exceptions import *
from .util import *
from .result import *

logger = logging.getLogger(__name__)

def dict_get(d, k, de):
    if not k in d:
        d[k] = de
    return d[k]

class _Rule(object):
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

    def f_in(self, makecall):
        """
        This function must be defined by a derived class.
        It must return a generator of 

        :param MakeCall makecall: makecall object
        """
        raise NotImplementedError()

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
            print("error in print_dep of {}".format(repr(self)))
            print(e)

    def output_exists(self):
        return False

    def output_mtime(self):
        return None

    def make_ancestors(self, makecall, test):

        makecall2 = makecall.copy()
        makecall2.args['test'] = test

        for f in self.f_in(makecall):

            if f is None:
                raise Exception("None in f_in {}".format(self))

            r = makecall2.make(f, self)

            if not isinstance(r, Result):
                print(f)
                print(r)
                raise Exception()

            yield f

    def check(self, makecall, test=False):
        
        if makecall.args.get('force', False): return True, None

        f_in = list(self.make_ancestors(makecall, test))
        
        b = self.output_exists()
        if not b:
            return True, "output does not exist"

        mtime = self.output_mtime()
        
        for f in f_in:
            if isinstance(f, Rule):
                continue
            
            if not isinstance(f, Req):
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

    def make(self, makecall, req):
        #magenta("make {}".format(self))

        if self.up_to_date: 
            raise Exception()
            return
        
        #f_out = list(self.rule_f_out())
       
        if req.would_touch(makecall):
            should_build, f = self.check(makecall, test=True)
            if should_build:
                req.touch_maybe(makecall)
                return ResultBuild()
            else:
                return ResultNoBuild()
        
        should_build, f = self.check(makecall)
        
        try:
            f_in = list(self.f_in(makecall))
        except Exception as e:
            print(self)
            traceback.print_exc()
            raise e

        if should_build:
            if makecall.args.get('test', False):
                #print(crayons.blue('build {} because {}'.format(repr(self), f)))
                print('build {} because {}'.format(repr(self), f))
                return ResultTestBuild()
            else:
                #blue('build {} because {}'.format(repr(self), f))
                try:
                    ret = self.build(makecall, None, f_in)
                except Exception as e:
                    print(crayons.red('error building {}: {}'.format(repr(self), repr(e))))
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

    def write_binary(self, filename, b):
        # it appears that this functionality is actually not useful as currently
        # implemented. revisit later

        #if check_existing_binary_data(filename, b):
        if True:
            pymake.makedirs(os.path.dirname(filename))
            with open(filename, 'wb') as f:
                f.write(b)
        else:
            print('binary data unchanged. do not write.')

    def rules(self, makecall):
        yield self
        for r in self._rules(makecall):
            yield from r.rules(makecall)

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
            red(e)
            red(repr(req))
            red(repr(req.fn))
            red(repr(pat))
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
        return 'pymake.RuleRegex{}'.format((self.f_out, self.groups))

        
class RuleRegexSimple(RuleRegex):
    def build(self, makecall, _, f_in):
        print(crayons.yellow("Build " + repr(self),'yellow',bold=True))

        pymake.makedirs(os.path.dirname(self.f_out))

        if f_in[0].fn[-3:] == ".py":
            cmd = [sys.executable, f_in[0].fn, self.f_out] + [a.fn for a in f_in[1:]]
        else:
            prog = f_in[0].fn
            out = os.path.abspath(self.f_out)
            cmd = [prog, out] + [os.path.abspath(a.fn) for a in f_in[1:]]

        print(crayons.yellow("cmd = {}".format(' '.join(cmd)),'yellow',bold=True))

        subprocess.run(cmd, check=True)

class RuleRegexSimple2(RuleRegex):
    def build(self, makecall, _, f_in):
        print(crayons.yellow("Build " + repr(self),'yellow',bold=True))

        pymake.makedirs(os.path.dirname(self.f_out))

        assert f_in[0].fn[-3:] == ".py"
        
        av = [f_in[0].fn, self.f_out] + [a.fn for a in f_in[1:]]
        
        s = f_in[0].fn[:-3].replace('/','.')
        m = __import__(s, fromlist=['main'])
        
        print(s)
        print(m)
        print(m.main)

        m.main(makecall, av)


