import asyncio
import contextlib
import functools
import hashlib
import inspect
import re
import os
import logging
import traceback
import json

import crayons
import pygraphviz as gv

from mybuiltins import *
from .util import *

import pymake.args
import pymake.rules

logger = logging.getLogger(__name__)

class MakeCall:
    def __init__(self, makefile, args={}, graph={}, stack=[]):
        self.makefile = makefile
        self.decoder = makefile.decoder

        self.args = pymake.args.Args(**args)

        self.graph = graph

        self.stack = stack

    @property
    def show_plot(self):
        return self.args.show_plot

    def copy(self, **kwargs):
        args1 = dict(self.args._args)
        args1.update(kwargs)
        return MakeCall(self.makefile, args1, self.graph, self.stack)

    def make_threadsafe(self, *args, **kwargs):
        loop = asyncio.new_event_loop()
        return loop.run_until_complete(self.make(*args, **kwargs))

    async def make(self, req, test=None, ancestor=None, **kwargs):
        """
        this is the ONLY make function that should be called outside the pymake module
        """

        assert (req is not None) and isinstance(req, pymake.req.Req)
        if isinstance(req, pymake.req.ReqFake): return pymake.result.ResultNoBuild()

        # check cache
        req1 = self.makefile.check_cache(req)
        if req1 is not None:
            logger.debug(crayons.green(f"cached req found {req!r} {req1._up_to_date}"))
            req = req1
        else:
            self.makefile.reqs.append(req)

        
        if req._up_to_date: return pymake.result.ResultNoBuild()

        logger.debug(repr(req))

        # added this because needed to make a file when test was True
        if test is None:
            test = self.args.test

        makecall = self.copy(test=test, **kwargs)

        with MakeContext(makecall.stack, req):

            if not req.build:
                if req.output_exists():
                    ret = pymake.result.ResultNoBuild()
                    logger.info(f"make {req} result = {ret}")
                    return ret

            if isinstance(req, pymake.rules.Rule):
                return await req.make(mc, None)

            req = self.ensure_is_req(req)
    
            ret = await req._make(makecall, ancestor)

            logger.debug(f"make {req} result = {ret}")
            return ret

    def ensure_is_req(self, target):
        if isinstance(target, str):
            target = pymake.req.ReqFile(target)

        if not isinstance(target, pymake.req.Req):
            raise Exception('Excepted Req, got {}'.format(repr(target)))

        return target

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


