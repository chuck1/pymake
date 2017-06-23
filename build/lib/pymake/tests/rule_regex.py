import os
import re
import tempfile
import unittest

import pymake

d = "build/tests"

try:
    os.makedirs(d)
except: pass

class RuleRegex(pymake.RuleRegex):
    pat_out = re.compile(d + "/(\w+)\.b")

    def f_in(self, makecall):
        yield d + "/" + self.groups[0] + ".a"

    def build(self, makecall, f_out, f_in):
        print(f_out)
        print(f_in)

class TestRuleRegex(unittest.TestCase):
    def test(self):
        
        m = pymake.Makefile()
            
        m.rules.append(RuleRegex)
            
        with open(d + '/hello.a', 'w') as f:
            f.write('hello')

        m.make(d + '/hello.b')


