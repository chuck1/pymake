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
    def complete(self):
        return True
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

    def check(self, makecall):
        #magenta("check {}".format(self))
        
        if makecall.force: return True, None

        f_in = []
        
        for f in self.f_in(makecall):
            f_in.append(f)

            if f is None:
                #return True, "None in f_in {}".format(self)
                raise Exception("None in f_in {}".format(self))

            #if callable(f):
            #    raise RuntimeError('deprecated ' + str(f))
            #    makecall.make(f())
            #else:
            r = makecall.make(f, self)
            
        try:
            b = self.output_exists()
        except OutputNotExists as e:
            return True, str(e)
        else:
            if not b:
                return True, "output does not exist"

        mtime = self.output_mtime()
        
        for f in f_in:
            if isinstance(f, Rule):
                #return True, "Rule object in f_in {}".format(f)
                continue
            
            if not isinstance(f, Req):
                raise Exception('{} f_in should return generator of Req objects, not {}'.format(repr(self), type(f)))

            if mtime is None:
                return True, '{} does not define mtime'.format(self.__class__.__name__)
            else:
                try:
                    b = f.output_exists()
                except OutputNotExists: pass
                else:
                    if b:
                        mtime_in = f.output_mtime()

                        if mtime_in is None:
                            return True, 'input file {} does not define mtime'.format(repr(self))
                    
                        if mtime_in > mtime:
                            return True, 'mtime of {} is greater'.format(f)
                        else:
                            #green('up-to-date compared to {}'.format(f))
                            pass
        
        return False, None

    def make(self, makecall):
        #magenta("make {}".format(self))

        if self.up_to_date: return
        
        #f_out = list(self.rule_f_out())
        
        should_build, f = self.check(makecall)
        
        try:
            f_in = list(self.f_in(makecall))
        except Exception as e:
            print(self)
            traceback.print_exc()
            raise e

        if should_build:
            if makecall.test:
                #print(crayons.blue('build {} because {}'.format(repr(self), f)))
                print('build {} because {}'.format(repr(self), f))
            else:
                #blue('build {} because {}'.format(repr(self), f))
                try:
                    ret = self.build(makecall, None, f_in)
                except Exception as e:
                    print(crayons.red('error building {}: {}'.format(repr(self), repr(e))))
                    raise

                if ret is None:
                    pass
                elif ret != 0:
                    raise BuildError(str(self) + ' return code ' + str(ret))
        else:
            if makecall.test:
                #print('DONT build',repr(self))
                pass

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

class RuleDocAttr(_Rule):
    """
    requires class attributes:

    - ``pat_id``
    - ``attrs``

    Example:
    
    .. testcode::
        
        from pymake import Makefile, RuleDocAttr, ReqDocAttr
        from pymake.mongo import Collection, DocumentContext

        class RuleMongo(RuleDocAttr):
            pat_id = re.compile('doc1')
            attrs = set(('a', 'b', 'c'))
        
            def build(self, makecall, f_out, f_in):
                with DocumentContext(makecall.makefile.coll.collection, (self._id,)) as (doc,):
                    doc['a'] = 1
                    doc['b'] = 2
                    doc['c'] = 3
                        
            def f_in(self, makecall):
                yield ReqDocAttr('doc1', {})
    
        m = Makefile()
        m.coll = Collection(('localhost', 27017), 'db_name', 'collection_name')
    """
    def __init__(self, req, groups):
        super(RuleDocAttr, self).__init__()
        self.id_ = req.id_
        self.req = req

    def complete(self):
        return not bool(self.req.attrs_remain)

    def __repr__(self):
        return '<{}.{} pat_id={} attrs={}>'.format(self.__class__.__module__, self.__class__.__name__, self.pat_id, self.attrs)

    @classmethod
    def test(cls, req):
        if not isinstance(req, ReqDocAttr): return None

        m = cls.pat_id.match(req.id_)

        #print(cls.pat_id, req.id_)

        if m is None: return None
        
        #green('id mathces')

        if not (cls.attrs & req.attrs_remain): return None
        
        #green('RuleDocAttr match was attrs_remain = {}'.format(req.attrs_remain))
        #green('rule provides {}'.format(cls.attrs))
        
        req.attrs_remain = req.attrs_remain - cls.attrs

        #green('RuleDocAttr match now attrs_remain = {}'.format(req.attrs_remain))

        return cls(req, m.groups())

class RuleFileAttr(_Rule):
    """
    requires class attributes:

    - ``pat_out``
    - ``attrs``

    Example:
    
    .. testcode::
        
        class Rule(RuleDocAttr):
            pat_id = re.compile('doc1')
            attrs = set(('a', 'b', 'c'))
        
            def build(self, makecall, f_out, f_in):
                with DocumentContext(makecall.makefile.coll.collection, (self._id,)) as (doc,):
                    doc['a'] = 1
                    doc['b'] = 2
                    doc['c'] = 3
                        
            def f_in(self, makecall):
                yield ReqDocAttr('doc1', {})
    
    """
    def __init__(self, req, groups):
        super(RuleFileAttr, self).__init__()
        self.req = req
        self.groups = groups
 
    #def output_exists(self):
    #    return self.req.output_exists()

    def output_exists(self):
        """
        check if the file exists
        """
        if not os.path.exists(self.req.id_):
            #red('does not exist: {}'.format(self.id_))
            return False

        for a in self.attrs:
            try:
                m = get_meta(self.obj, a)
                if not isinstance(m, dict):
                    set_meta(self.obj, a, {'mtime':0})
                    #red('meta for {} = {}'.format(a, m))
            except NoMeta:
                raise OutputNotExists("No meta data for {} in {}".format(repr(a), repr(self.obj)))
        
        return True

    @cached_property
    def obj(self):
        with open(self.req.id_, 'rb') as f:
            return pickle.load(f)

    #def output_mtime(self):
    #    #red('req={}'.format(self.req))
    #    t = self.req.output_mtime()
    #    #red('return {}'.format(t))
    #    return t

    def output_mtime(self):
        """
        return the mtime of the file
        """
        mtimes = []

        for a in self.attrs:
            try:
                m = get_meta(self.obj, a)
            except Exception as e:
                red("error in output_mtime of {}".format(repr(self)))
                red(repr(e))
                raise

            #red('meta for {} = {}'.format(a, m))
            mtimes.append(m['mtime'])

        ret = max(mtimes)

        #red('{} mtimes is {}...{}'.format(self, mtimes, ret))

        return ret
   
    def complete(self):
        return not bool(self.req.attrs_remain)

    def __repr__(self):
        return '<{}.{} pat_out={} attrs={}>'.format(self.__class__.__module__, self.__class__.__name__, self.pat_out, self.attrs)

    @classmethod
    def test(cls, req):
        if not isinstance(req, ReqFileAttr): return None

        m = cls.pat_out.match(req.id_)

        if m is None:
            return None
        
        #green('id mathces')

        if not (cls.attrs & req.attrs_remain): return None
        
        #green('RuleDocAttr match was attrs_remain = {}'.format(req.attrs_remain))
        #green('rule provides {}'.format(cls.attrs))
        
        req.attrs_remain = req.attrs_remain - cls.attrs

        #green('RuleDocAttr match now attrs_remain = {}'.format(req.attrs_remain))

        return cls(req, m.groups())
    
    def context(self, factory=None):
        return FileContext(self, factory)

class Proxy(object):
    def __init__(self, o):
        object.__setattr__(self, '_o', o)

    def __getattribute__(self, name):
        o = object.__getattribute__(self, '_o')
        v = getattr(o, name)
        
        try:
            import esolv.storage
            if isinstance(v, esolv.storage.Storage):
                v = Proxy(v)
        except Exception as e:
            print(e)
        
        return v
    
    def __setattr__(self, name, value):
        o = object.__getattribute__(self, '_o')
        setattr(o, name, value)
        
        if not hasattr(o, '_meta_attr'):
            o._meta_attr = {name:{}}
        else:
            if name not in o._meta_attr:
                o._meta_attr[name] = {}
            elif not isinstance(o._meta_attr[name], dict):
                o._meta_attr[name] = {}

        m = o._meta_attr[name]
        
        m['mtime'] = time.time()


class FileContext:
    def __init__(self, rule, factory=None):
        self.rule = rule
        self.factory = factory

    def __enter__(self):
        if os.path.exists(self.rule.req.id_):
            with open(self.rule.req.id_, 'rb') as f:
                self.o = pickle.loads(f.read())
        else:
            self.o = self.factory()
        
        p = Proxy(self.o)

        return p

    def __exit__(self, exc_type, exc, tb):
        
        if exc_type:
            red("error in FileContext {} {}".format(exc_type, exc))
            return False

        b = pickle.dumps(self.o)
        
        pymake.makedirs(os.path.dirname(self.rule.req.id_))

        with open(self.rule.req.id_, 'wb') as f:
            f.write(b)
        
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


