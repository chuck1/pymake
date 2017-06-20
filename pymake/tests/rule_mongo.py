import re
import unittest
import pymongo
from pprint import pprint

import pymake

#client = pymongo.MongoClient('localhost', 27017)
#db = client.test_pymake
#coll = db.test_collection

class Collection(object):
    def __init__(self, address, db, coll):
        self.client = pymongo.MongoClient(*address)
        self.collection = self.client[db][coll]

    def insert_one_if_not_exists(self, _id):
        try:
            self.collection.insert_one({'_id': _id})
        except pymongo.errors.DuplicateKeyError: pass

class DocumentContext(object):

    class Doc(object):
        def __init__(self, context, doc):
            self.context = context
            self.doc = doc

        def __getitem__(self, arg):
            return self.doc.__getitem__(args)

        def __setitem__(self, arg, v):
            self.context.update(self.doc['_id'], {'$set': {arg: v}})
            return self.doc.__setitem__(arg, v)

    def __init__(self, collection, _ids):
        self.collection = collection
        self._ids = _ids

        self.bulk = self.collection.initialize_ordered_bulk_op()

    def update(self, _id, change):
        self.bulk.find({'_id': _id}).update(change)

    def __enter__(self):
        docs = tuple(DocumentContext.Doc(self, self.collection.find_one({'_id':_id})) for _id in self._ids)
        return docs

    def __exit__(self, exc_type, exc_value, tb):
        print(exc_type)
        if exc_type is None:
            self.bulk.execute()


class R0(pymake.RuleDocAttr):
    pat_id = re.compile('doc1')
    attrs = set()

    def build(self, makecall, f_out, f_in):
        print('R0 build')

        makecall.makefile.coll.insert_one_if_not_exists(self._id)

    def f_in(self, makecall):
        return 
        yield

class R1(pymake.RuleDocAttr):
    pat_id = re.compile('doc1')
    attrs = set(('a', 'b', 'c'))

    def build(self, makecall, f_out, f_in):
        print('R1 build')

        with DocumentContext(makecall.makefile.coll.collection, (self._id,)) as (doc,):
            doc['a'] = 1
            doc['b'] = 2
            doc['c'] = 3
                
    def f_in(self, makecall):
        yield pymake.ReqDocAttr('doc1', {})

class TestMongo(unittest.TestCase):
    def test(self):

        m = pymake.Makefile()

        m.coll = Collection(('localhost', 27017), 'test', 'test')
        m.coll.collection.delete_many({})
       
        m.rules.append(R0)
        m.rules.append(R1)
       
        m.make(pymake.ReqDocAttr('doc1', set(('a',))))

        doc = m.coll.collection.find_one({'_id':'doc1'})
        pprint(doc)
        self.assertEqual(doc['a'], 1)
        self.assertEqual(doc['b'], 2)
        self.assertEqual(doc['c'], 3)


