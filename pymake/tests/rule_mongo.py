import re
import unittest
import pymongo
from pprint import pprint

import pymake
import pymake.mongo

class R0(pymake.RuleDocAttr):
    pat_id = re.compile('doc1')
    attrs = set()

    def build(self, makecall, f_out, f_in):

        makecall.makefile.coll.insert_one_if_not_exists(self._id)

    def f_in(self, makecall):
        return 
        yield

class R1(pymake.RuleDocAttr):
    pat_id = re.compile('doc1')
    attrs = set(('a', 'b', 'c'))

    def build(self, makecall, f_out, f_in):
        with pymake.mongo.DocumentContext(makecall.makefile.coll.collection, (self._id,)) as (doc,):
            doc['a'] = 1
            doc['b'] = 2
            doc['c'] = 3
                
    def f_in(self, makecall):
        yield pymake.ReqDocAttr('doc1', {})

class TestMongo(unittest.TestCase):
    def test(self):

        m = pymake.Makefile()
        m.coll = pymake.mongo.Collection(('localhost', 27017), 'test', 'test')
        m.coll.collection.delete_many({})
       
        m.rules.append(R0)
        m.rules.append(R1)
       
        m.make(pymake.ReqDocAttr('doc1', set(('a',))))

        doc = m.coll.collection.find_one({'_id':'doc1'})

        self.assertEqual(doc['a'], 1)
        self.assertEqual(doc['b'], 2)
        self.assertEqual(doc['c'], 3)


