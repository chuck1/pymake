import inspect
import os
import re
import sys
import types
from mybuiltins import *
import pymake

def _isinstance(v, cls):
    if not inspect.isclass(v): return False
    if not issubclass(v, cls): return False
    if v is cls: return False
    return True

def _search(root_path, m, cls, _rules, searched):

    if m in searched: return _rules
    searched.append(m)

    if not hasattr(m, '__file__'): return _rules

    #root_path = 'coil_testing/rules'

    module_path = m.__file__
    if isinstance(module_path, list):
        assert len(module_path) == 1
        module_path = module_path[0]

    if not os.path.relpath(module_path).startswith(root_path): return _rules

    for k, v in m.__dict__.items():
        if isinstance(v, types.ModuleType):
            if v is pymake: continue
            _search(root_path, v, cls, _rules, searched)

        elif _isinstance(v, cls):
            _rules.append(v)

        #elif inspect.isclass(v):
        if False:
           
            if False:
                print(v)
                print(v, '\t', issubclass(v, cls))
                print(v, '\t', isinstance(v, cls))
                print(v, '\t', cls in inspect.getmro(v))
                for c in inspect.getmro(v):
                    print(v, '\t\t', c, cls == c, cls.__name__, c.__name__)


            if issubclass(v, cls):
                if not (v is cls):
                    _rules.append(v)
    
    return _rules

def search(m, cls):

    root_path = os.path.split(os.path.relpath(m.__file__))[0]

    return _search(root_path, m, cls, [], [])

if __name__=='__main__':

    m = __import__(sys.argv[1])
    
    t = eval(sys.argv[2])

    r = search(m, t)

    print(r)

