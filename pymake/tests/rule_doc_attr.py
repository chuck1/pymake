
import unittest

import pymake

docs = {'doc1': {}}

class R0(pymake.RuleDocAttr):
    pat_docpath = 'doc1'
    attrs = set()

    def build(self, makecall, f_out, f_in):
        print('R0 build')

        docs[self.docpath] = {}

    def f_in(self, makecall):
        return 
        yield

class R1(pymake.RuleDocAttr):
    pat_docpath = 'doc1'
    attrs = set(('a', 'b', 'c'))

    def build(self, makecall, f_out, f_in):
        print('R1 build')

        doc = docs[self.docpath]

        doc['a'] = 1
        doc['b'] = 2
        doc['c'] = 3

        print('docpath=',self.docpath)
        print('doc=',doc)
        

    def f_in(self, makecall):
        yield pymake.ReqDocAttr('doc1', {})

class Test(unittest.TestCase):
    def test(self):
        
        m = pymake.Makefile()

        m.rules.append(R0)
        m.rules.append(R1)
       
        m.make(pymake.ReqDocAttr('doc1', {'a'}))

        print(docs)


