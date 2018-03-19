import contextlib
import functools
import inspect
import re
import os
import logging
import traceback

import crayons
import pygraphviz as gv

from .util import *

logger = logging.getLogger(__name__)

class MakeCall:
    def __init__(self, makefile, args, graph={}, stack=[]):
        self.makefile = makefile
        self.args = args

        self.graph = graph

        self.stack = stack

    @property
    def show_plot(self):
        return self.args.get('show_plot', False)

    def copy(self):
        return MakeCall(self.makefile, dict(self.args), self.graph, self.stack)

    def make(self, target, ancestor=None):
        assert(target is not None)

        with MakeContext(self.stack, target):
            #print(crayons.blue("stack = {}".format(self.stack), bold = True))
            return self.makefile._make(self, target, ancestor)

    def add_edge(self, r1, r2):
        if r1 is None: return

        try:
            v1 = dict_get(self.graph, r1.f_out, {})
            v2 = dict_get(v1, r2.f_out, {})
        except Exception as e:
            print(e, repr(e))

    def render_graph(self):
        print(crayons.magenta('render graph', bold=True))
        g = gv.AGraph(directed=True, rankdir='LR')

        def f1(n, d, f):
            for k, v in d.items():
                g.add_node(k)
                if n:
                    g.add_edge(n, k)
                f(k, v, f1)
        
        f1(None, self.graph, f1)
        
        with open('layout.dot', 'w') as f:
            f.write(g.string())


