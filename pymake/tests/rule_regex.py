
import tempfile
import unittest

import pymake

class RuleRegex1(pymake.RuleRegex2):
    def __init__(self, d):
        super(RuleRegex1, self).__init__(self.f_out, self.f_in, self.build)
        self.d = d

    def f_out(self):
        yield self.d + "/(\w+)\.b"

    def f_in(self, makecall):
        def path(w):
            return self.d + "/" + w + ".a"

        yield path

    def build(self, makecall, f_out, f_in):
        print(f_out)
        print(f_in)

class TestRuleRegex(unittest.TestCase):
    def test(self):
        
        with tempfile.TemporaryDirectory() as d:

            m = pymake.Makefile()
            
            m.rules.append(RuleRegex1(d))
            
            with open(d + '/hello.a', 'w') as f:
                f.write('hello')

            m.make(d + '/hello.b')


