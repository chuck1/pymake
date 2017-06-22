__version__ = '0.2a0'

import inspect
import re
import os
import logging
import traceback

import pymake.os0

logger = logging.getLogger(__name__)

"""
The pymake module
"""

MONGO_COLLECTION = None

def bin_compare(b0,b1):
    for c0,c1 in zip(b,b1):
        try:
            s0 = chr(c0)
            s1 = chr(c1)
        except Exception as e:
            print(e)
            s0 = ''
            s1 = ''
        
        msg = '' if c0==c1 else 'differ'
        
        print("{:02x} {:02x} {:6} {:6} {}".format(c0,c1,repr(s0),repr(s1),msg))

def check_existing_binary_data(filename, b0):
    if os.path.exists(filename):
        with open(filename, 'rb') as f:
            b1 = f.read()
        
        if b0 == b1:
            return False
        else:
            #bin_compare(b0,b1)
            return True
    else:
        return True

class BuildError(Exception):
    def __init__(self, message):
        super(BuildError, self).__init__(message)

class NoTargetError(Exception):
    def __init__(self, message):
        super(NoTargetError, self).__init__(message)

class MakeCall(object):
    def __init__(self, makefile, test=False, force=False, show_plot=False):
        self.makefile = makefile
        self.test = test
        self.force = force
        self.show_plot = show_plot
    
    def make(self,t):
        self.makefile.make(t, self.test, self.force)

class Makefile(object):
    """
    manages the building of targets
    """
    def __init__(self):
        self.rules = []

    def find_rule(self, target):
        for rule in self.rules:
            
            try:
                if inspect.isclass(rule):
                    r = rule.test(target)
                else:
                    r = rule.test(target)

            except:
                print(type(rule))
                print(rule)
                print(rule.test)
                print(target)
                raise
            
            if r is not None:
                return r
        return None

    def add_rules(self, generator):
        """
        code inside the generator may try to make files, so they expect the previously
        yielded rules to be available in self.rules
        """
        for r in generator:
            self.rules.append(r)

    def print_dep(self, target, indent=0):
        
        if isinstance(target, list):
            target = target[0]

        print(" " * indent + str(target))
        rule = self.find_rule(target)
        if rule is not None:
            rule.print_dep(MakeCall(self), indent + 2)

    def make(self, target, test=False, force=False, regex=False, show_plot=False):
        """
        :param test:  follow the file dependencies and print out which files would be built
                      and a short description of why check returned True. But do not
                      call the build function.
        :param regex: treat targets as regex expressions and make all targets that match
        """
        if regex:
            for t in self.search_gen(target):
                self.make(t, test, force)
            return

        if isinstance(target, list):
            for t in target: self.make(t, test, force)
            return
        
        if target is None:
            raise Exception('target is None'+str(t))

        if isinstance(target, Rule):
            target.make(MakeCall(self, test, force, show_plot=show_plot))
            return
        
        # at this point target should be a string representing a file (since we arent set up for DocAttr yet)
        
        if isinstance(target, str):
            target = ReqFile(target)
        
        if not isinstance(target, _Req):
            raise Exception('{}'.format(repr(target)))

        rule = self.find_rule(target)

        if rule is None:
            if target.output_exists():
                pass
            else:
                raise NoTargetError("no rules to make {}".format(repr(target)))
        else:
            try:
                rule.make(MakeCall(self, test, force))
            except NoTargetError as e:
                print('while building', repr(target))
                print(' ',e)
                raise


    def search_gen(self, target):
        if isinstance(target, list):
            for t in target:
                yield from self.search_gen(t)
            return
        
        pat = re.compile(target)
        
        for rule in self.rules:
            if not isinstance(rule, pymake.Rule): continue

            f_out = rule.f_out
            logger.debug('target={} f_out={}'.format(repr(target), repr(f_out)))
            m = pat.match(f_out)
            if m:
                yield f_out
        
    def search(self, t):
        if isinstance(t, list):
            for t1 in t: self.search(t1)
            return
        
        print('regex ',repr(t))

        pat = re.compile(t)
        
        for rule in self.rules:
            try:
                f_out = rule.f_out
            except:
                continue
            
            m = pat.match(f_out)
            if m:
                print(rule, f_out)
                #print(f, repr(rule))

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

    def check(self, makecall):
        
        if makecall.force: return True, None

        f_in = []
        
        for f in self.f_in(makecall):
            f_in.append(f)

            if f is None:
                return True, "None in f_in {}".format(self)
                raise Exception("None in f_in {}".format(self))

            if callable(f):
                makecall.make(f())
            else:
                makecall.make(f)
        
        if not self.output_exists():
            return True, "output does not exist"

        mtime = self.output_mtime()
        
        for f in f_in:
            if isinstance(f, Rule):
                #return True, "Rule object in f_in {}".format(f)
                continue
            
            if not isinstance(f, _Req):
                raise Exception('{} f_in should return generator of Req objects, not {}'.format(repr(self), type(f)))

            if mtime is None:
                return True, '{} does not define mtime'.format(self.__class__.__name__)
            else:
                if f.output_exists():
                    mtime_in = f.output_getmtime()

                    if mtime_in is None:
                        return True, '{} does not define mtime'.format(repr(self))
                    
                    if mtime_in > mtime:
                        return True, f
        
        return False, None

    def make(self, makecall):

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
                print('build',repr(self),'because',f)
            else:
                ret = self.build(makecall, None, f_in)
                if ret is None:
                    pass
                elif ret != 0:
                    raise BuildError(str(self) + ' return code ' + str(ret))
        else:
            if makecall.test:
                print('DONT build',repr(self))

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
            pymake.os0.makedirs(os.path.dirname(filename))
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
        super(RuleStatic, self).__init__(
                lambda: static_f_out,
                lambda makefile: static_f_in,
                func)

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

        m = cls.pat_out.match(req.fn)
        if m is None: return None
        return cls(req.fn, m.groups())

    def output_exists(self):
        return os.path.exists(self.target)

    def output_mtime(self):
        return os.path.getmtime(self.target)

    def __init__(self, target, groups):
        self.target = target
        self.groups = groups

        _Rule.__init__(self)

    def __repr__(self):
        return 'pymake.RuleRegex{}'.format((self.target, self.groups))

class _Req(object):
    def output_exists(self):
        return None

    def output_getmtime(self):
        return None

class ReqFile(_Req):
    def __init__(self, fn):
        self.fn = fn

    def output_exists(self):
        return os.path.exists(self.fn)

    def output_getmtime(self):
        return os.path.getmtime(self.fn)

    def __repr__(self):
        return 'pymake.ReqFile({})'.format(repr(self.fn))

class ReqDocAttr(_Req):
    """
    :param str _id: document id
    :param set attrs: set of attributes
    """

    def __init__(self, id_, attrs):
        self.id_ = id_
        self.attrs = attrs

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
    def __init__(self, _id, attrs, groups):
        super(RuleDocAttr, self).__init__()
        self._id = _id
        self.attrs = attrs

    @classmethod
    def test(cls, req):
        if not isinstance(req, ReqDocAttr): return None

        m = cls.pat_id.match(req.id_)

        print(cls.pat_id, req.id_)

        if m is None: return None
        
        if not cls.attrs.issuperset(req.attrs): return None

        return cls(req.id_, req.attrs, m.groups())
        





