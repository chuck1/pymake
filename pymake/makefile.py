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
import numpy

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

@contextlib.contextmanager
def render_graph_on_exit(mc):
    try:
        yield
    finally:
        mc.render_graph()

class Makefile:


    """
    manages the building of targets
    """
    def __init__(self):
        self.rules = []
        self._rules_doc = {}

        # cache req files
        # when a req is made, the req and its requirements be stored here
        # whenever a req is made, the cache will be checked and if an matching req is there, it will be 
        # used instead.
        # say req A depends on req B
        # we make req A, which leads to making req B
        # both are stored in the cache
        # req A stores a bool that says its up to date and creates a signal for req B that will be called if
        # req B gets updated with the program is still running
        self.reqs = []
    
    def check_cache(self, req):
        for req1 in self.reqs:
            if req1 == req:
                return req1

    async def find_one(self, mc, target):
        async for r in self.find_rule(mc, target):
            assert isinstance(r, pymake.rules._Rule)
            return r

        #raise Exception('no rule to make target {}'.format(repr(target)))

    async def find_rule(self, mc, target):
        
        mc1 = mc.copy(force=False)

        def _rules_to_check():
            if isinstance(target, pymake.req.req_doc.ReqDocBase):
                if target.d['type'] not in self._rules_doc:
                    #print_lines(logging.error, target.print_long)
                    #raise Exception(f"no rules to make {target!r}")
                    return

                for r in self._rules_doc[target.d['type']]:
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

                if pat['type'] not in self._rules_doc:
                    self._rules_doc[pat['type']] = []

                self._rules_doc[pat['type']].append(r)
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
            
        rules = await alist(self.find_rule(mc, target))

        if len(rules) > 1:
            if all([isinstance(r, RuleRegex) for r in rules]):
                l = [sum(len(g) for g in r.groups) for r in rules]
                #green(l)
                i = numpy.argsort(l)
                #green(i)
                rules = numpy.array(rules)[i]
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


