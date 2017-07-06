import re
import unittest

import pymake

docs = {'doc1': {}}

class R0(pymake.RuleDocAttr):
    pat_id = re.compile('doc1')
    attrs = set(('a',))

    def f_in(self, makecall):
        return 
        yield

    def build(self, makecall, f_out, f_in):
        print('R0 build')

        docs[self.id_] = {'a': 1}

class R1(pymake.RuleDocAttr):
    pat_id = re.compile('doc1')
    attrs = set(('b', 'c'))

    def f_in(self, makecall):
        yield pymake.ReqDocAttr('doc1', set(('a',)))

    def build(self, makecall, f_out, f_in):
        print('R1 build')

        doc = docs[self.id_]

        doc['b'] = 2
        doc['c'] = 3

def test():
    
    m = pymake.Makefile()

    m.rules.append(R0)
    m.rules.append(R1)
   
    m.make(pymake.ReqDocAttr('doc1', set(('a',))))

    print(docs)




