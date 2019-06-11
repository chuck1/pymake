import contextlib
import functools
import inspect
import re
import os
import logging
import traceback
from pprint import pprint
import sys
import bson
#import numpy

#from mybuiltins import *
from .rules import *
from .util import *
from .makecall import *
import pymake.result
import pymake.req

logger = logging.getLogger(__name__)

def touch(fname, times=None):
    with open(fname, 'a'):
        os.utime(fname, times)

#@contextlib.contextmanager
class render_graph_on_exit:
    def __init__(self, mc):
        self.mc = mc
    def __enter__(self):
        return
    def __exit__(self, *args):
        self.mc.makefile.render_graph()

class CacheReq:
    def __init__(self):
        self.__cache = {}
        self.__cache_1 = []

    def _find_in_list(self, req, lst):
        for req1 in lst:
            if req1 == req:
                logger.debug(crayons.green(f"cached req found {req!r} up_to_date_0={req1.up_to_date_0} up_to_date_1={req1.up_to_date_1}"))
                return req1

        lst.append(req)
        return req

    def find(self, req):

        if not isinstance(req, pymake.req.req_doc.ReqDocBase):
            return self._find_in_list(req, self.__cache_1)

        def _hash_func(req):
            return req.hash

        h = _hash_func(req)

        if h not in self.__cache:
            self.__cache[h] = []
            return req

        subCache = self.__cache[h]

        logger.debug(f'subCache len: {len(subCache):4} h: {h!s:32}')

        return self._find_in_list(req, subCache)

class Makefile:

    """
    manages the building of targets
    """
    def __init__(self, 
            registry=None,
            req_cache=None,
            decoder=None):
        self.rules = []
        self._rules_doc = {}

        #assert decoder is not None
        #self.decoder = decoder

        # cache req files
        # when a req is made, the req and its requirements be stored here
        # whenever a req is made, the cache will be checked and if an matching req is there, it will be 
        # used instead.
        # say req A depends on req B
        # we make req A, which leads to making req B
        # both are stored in the cache
        # req A stores a bool that says its up to date and creates a signal for req B that will be called if
        # req B gets updated with the program is still running
        self.__cache = CacheReq()

        self.registry = registry

        self.graph = {}

    def cache_contains(self, req):
        for req1 in self.__reqs:
            if req1 == req:
                return True
        return False

    def cache_get(self, req):
        return self.__cache.find(req)

    async def find_one(self, mc, target):
        async for r in self.find_rule(mc, target):
            assert isinstance(r, pymake.rules._Rule)
            return r

        #raise Exception('no rule to make target {}'.format(repr(target)))

    async def find_rule(self, mc, target):
        
        mc1 = mc.copy(force=False)

        def _rules_to_check():
            if isinstance(target, pymake.req.req_doc.ReqDocBase):
                if target.type_ not in self._rules_doc:
                    #print_lines(logging.error, target.print_long)
                    #raise Exception(f"no rules to make {target!r}")
                    return

                for r in self._rules_doc[target.type_]:
                    assert pymake.util._isinstance(r, pymake.rules._Rule)
                    yield r
            else:
                for r in self.rules:
                    assert pymake.util._isinstance(r, pymake.rules._Rule)
                    yield r

        for rule in _rules_to_check():
            try:
                r = await rule.test(mc1, target)
            except:
                print(f'type(rule) = {type(rule)}')
                print(f'rule       = {rule}')
                print(f'rule.test  = {rule.test}')
                print(f'target     = {target}')
                raise
            
            if r is not None:
                r.req_out = target
                assert pymake.util._isinstance(r, pymake.rules._Rule)
                yield r

    def add_rules(self, generator):
        """
        code inside the generator may try to make files, so they expect the previously
        yielded rules to be available in self.rules
        """
        for r in generator:

            assert pymake.util._isinstance(r, pymake.rules._Rule)

            try:
                mro = inspect.getmro(r)
            except:
                mro = []

            if pymake.rules.RuleDoc in mro:
                pat = r.descriptor_pattern()

                # TODO only all 'type' are switched to 'type_', remove this
                type_ = pat['type_'] if 'type_' in pat else pat['type']

                if type_ not in self._rules_doc:
                    self._rules_doc[type_] = []

                logger.debug(f'add rule: {r!r}')

                self._rules_doc[type_].append(r)
            else:
                self.rules.append(r)

    def print_dep(self, target, indent=0):
        
        if isinstance(target, list):
            target = target[0]

        print(" " * indent + str(target))
        
        for rule in self.find_rule(target):
            rule.print_dep(MakeCall(self), indent + 2)

    async def make(self, target, **args):
        """
        :param test:  follow the file dependencies and print out which files would be built
                      and a short description of why check returned True. But do not
                      call the build function.
        :param regex: treat targets as regex expressions and make all targets that match
        """
        
        d = (set(args.keys()) - {'test', 'force', 'regex', 'show_plot', 'touch', 'list', 'verbose', 'search', 'doc', 'stop', 'id'})
        if d:
            #raise Exception(f'unexpected keyword arguments: {d}')
            pass


        self.args = args
        
        mc = MakeCall(self, args)
        
        with render_graph_on_exit(mc):

            if args.get('regex', False):
                print('regex')
                args = dict(kwargs)
                args.update({'regex':False})
                for t in self.search_gen(target):
                    await self._make(mc, t, None)

            elif args.get('doc', False):
                d = json.loads(target[0])
                r = pymake.req.req_doc.ReqDoc0(d)
                logger.info('make')
                for line in lines(r.print_long): logger.info(line)
                await mc.make(r)#, None)

            elif args.get('id', False):
                r = pymake.req.req_doc.ReqDoc2(
                        pymake.req.clean_doc(
                            pymake.req.client.find_one({'_id': bson.objectid.ObjectId(target[0])})))
                await mc.make(r)

            elif isinstance(target, list):
                for t in target:
                    await self._make(mc, t, None)

            else:
                await mc.make(target)


    async def rules_sorted(self, mc, target):
        
        logger.debug(f"{target}")

        rules = await alist(self.find_rule(mc, target))

        if len(rules) > 1:
            if all([isinstance(r, RuleRegex) for r in rules]):
                raise Exception()
                #l = [sum(len(g) for g in r.groups) for r in rules]
                #green(l)
                #i = numpy.argsort(l)
                #green(i)
                #rules = numpy.array(rules)[i]
                #green(rules)
        else:
            #green('exactly one matching rule found')
            pass
    
        return rules

    def search_gen(self, target):
        if isinstance(target, list):
            for t in target:
                yield from self.search_gen(t)
            return
        
        pat = re.compile(target)
        
        for rule in self.rules:
            if not isinstance(rule, Rule): continue

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

    def add_edge(self, r1, r2):
        if r1 is None: return

        try:
            v1 = dict_get(self.graph, r1.graph_string(), {})
            v2 = dict_get(v1, r2.graph_string(), {})
        except Exception as e:
            logger.error(f'{e!r}')
            raise

    def render_graph(self):
        print(crayons.magenta('render graph', bold=True))
        g = gv.AGraph(directed=True, rankdir='LR')

        def safe_label(s0):
            s1 = s0.replace('\n', '\\l')
            s2 = s1.replace('\\"','\'')
            #breakpoint()
            return s2

        def f1(n, d, f):
            if n:
                h0 = hashlib.md5(n.encode()).hexdigest()

            for k, v in d.items():
                
                h = hashlib.md5(k.encode()).hexdigest()

                g.add_node(h, label=safe_label(k))
                #print(f'h={h!r}')
                #print(f'k={k!r}')

                if n:
                    g.add_edge(h0, h)

                f(k, v, f1)
        
        f1(None, self.graph, f1)
        
        g.write('build/layout.dot')


