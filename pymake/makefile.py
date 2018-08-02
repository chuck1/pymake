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

    _cache_req = []

    """
    manages the building of targets
    """
    def __init__(self):
        self.rules = []
    
    async def find_one(self, mc, target):
        async for r in self.find_rule(mc, target):
            return r

        #raise Exception('no rule to make target {}'.format(repr(target)))

    async def find_rule(self, mc, target):
        mc1 = mc.copy(force=False)
        for rule in self.rules:
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
                r = pymake.req.ReqDoc(d)
                logger.info('make')
                print_lines(logger.info, r.print_long)
                await self._make(mc, r, None)

            elif args.get('id', False):
                r = pymake.req.ReqDoc(
                        pymake.req.clean_doc(
                            pymake.req.client.find_one({'_id': bson.objectid.ObjectId(target[0])})))
                await self._make(mc, r, None)

            elif isinstance(target, list):
                for t in target:
                    await self._make(mc, t, None)

            else:
                await self._make(mc, target, None)

    def ensure_is_req(self, target):
        if isinstance(target, str):
            target = pymake.req.ReqFile(target)

        if not isinstance(target, pymake.req.Req):
            raise Exception('Excepted Req, got {}'.format(repr(target)))

        #if isinstance(target, pymake.req.ReqFile):
        #    if not isinstance(target, pymake.req.ReqDoc):
        #        pat = re.compile('data/index/([0-9a-f]+)/(\d+)(\.\w+)')
        #        m = pat.match(target.fn)
        #        if m:
        #            d = file_index.manager.get_descriptor(m.group(1), m.group(2))
        #            return pymake.req.ReqFileDescriptor(d, m.group(3))

        return target

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

    async def _make(self, mc, req, ancestor):

        if req is None:
            raise Exception('req is None'+str(req))

        if not req.build:
            if req.output_exists():
                return pymake.result.ResultNoBuild()

        #print(crayons.magenta(str(target), bold=True))
        
        if isinstance(req, Rule):
            return await req.make(mc, None)
        
        # at this point target should be a string representing a file (since we arent set up for DocAttr yet)

        req = self.ensure_is_req(req)

        return await req.make(self, mc, ancestor)
        
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


