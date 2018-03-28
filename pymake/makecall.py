import contextlib
import functools
import hashlib
import inspect
import re
import os
import logging
import traceback

import crayons
import pygraphviz as gv

from mybuiltins import *
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
            v1 = dict_get(self.graph, r1.graph_string(), {})
            v2 = dict_get(v1, r2.graph_string(), {})
        except Exception as e:
            print(e, repr(e))

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
        
        g.write('layout.dot')
        return

        with open('layout.dot', 'w') as f:
            f.write(g.string())


