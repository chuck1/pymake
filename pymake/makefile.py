import contextlib
import functools
import inspect
import re
import os
import logging
import traceback
from pprint import pprint
import sys

import numpy

from .req import *
from .rules import *
from .util import *
from .makecall import *
from . import file_index

logger = logging.getLogger(__name__)

def touch(fname, times=None):
    with open(fname, 'a'):
        os.utime(fname, times)

class Makefile:

    _cache_req = []

    """
    manages the building of targets
    """
    def __init__(self):
        self.rules = []
    
    def find_one(self, target):
        for r in self.find_rule(target):
            return r

        #raise Exception('no rule to make target {}'.format(repr(target)))

    def find_rule(self, target):
        for rule in self.rules:
            try:
                r = rule.test(target)
            except:
                print(type(rule))
                print(rule)
                print(rule.test)
                print(target)
                raise
            
            if r is not None:
                r.req_out = target
                yield r

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
        
        for rule in self.find_rule(target):
            rule.print_dep(MakeCall(self), indent + 2)

    async def make(self, target, **args):
        """
        :param test:  follow the file dependencies and print out which files would be built
                      and a short description of why check returned True. But do not
                      call the build function.
        :param regex: treat targets as regex expressions and make all targets that match
        """
        
        d = (set(args.keys()) - {'test', 'force', 'regex', 'show_plot', 'touch', 'list', 'verbose', 'search', 'desc', 'stop'})
        if d:
            raise Exception(f'unexpected keyword arguments: {d}')

        mc = MakeCall(self, args)
        
        with mc.render_graph_on_exit():

            if args.get('regex', False):
                print('regex')
                args = dict(kwargs)
                args.update({'regex':False})
                for t in self.search_gen(target):
                    self._make(mc, t, None)

            elif args.get('desc', False):
                print(repr(target[0]))
                d = json.loads(target[0])
                self._make(mc, ReqFileDescriptor(d), None)

            elif isinstance(target, list):
                for t in target:
                    mc.make(t)

            else:
                self._make(mc, target, None)

    def ensure_is_req(self, target):
        if isinstance(target, str):
            target = ReqFile(target)

        if not isinstance(target, Req):
            raise Exception('{}'.format(repr(target)))

        if isinstance(target, ReqFile):
            if not isinstance(target, ReqFileDescriptor):
                pat = re.compile('data/index/([0-9a-f]+)/(\d+)(\.\w+)')
                m = pat.match(target.fn)
                if m:
                    d = file_index.manager.get_descriptor(m.group(1), m.group(2))
                    return ReqFileDescriptor(d, m.group(3))

        return target

    def rules_sorted(self, target):
            
        rules = list(self.find_rule(target))

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

    def _make(self, mc, target, ancestor):

        if target is None:
            raise Exception('target is None'+str(target))

        #print(crayons.magenta(str(target), bold=True))
        
        if isinstance(target, Rule):
            return target.make(mc, None)
        
        # at this point target should be a string representing a file (since we arent set up for DocAttr yet)

        target = self.ensure_is_req(target)

        return target.make(self, mc, ancestor)
        
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


