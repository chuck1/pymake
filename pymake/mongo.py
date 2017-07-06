import time

import pymongo

import pymake

class Collection(object):
    def __init__(self, address, db, coll):
        self.client = pymongo.MongoClient(*address)
        self.collection = self.client[db][coll]

    def insert_one(self, doc):
        self.collection.insert_one(doc)

    def find_one(self, doc):
        self.collection.find_one(doc)

    def insert_one_if_not_exists(self, _id):
        try:
            self.collection.insert_one({'_id': _id})
        except pymongo.errors.DuplicateKeyError: pass

    def doc_context(self, ids):
        return DocumentContext(self.collection, ids)

class DocumentContext(object):

    class Doc(object):
        def __init__(self, context, doc):
            self.context = context
            self.doc = doc

        def __getitem__(self, k):
            return self.doc[k]['_value']
        
        def __setitem__(self, k, v):
            if k in self.doc:
                s = self.doc[k]
                s['_value'] = v
            else:
                s = {'_value': v, '_meta': {}}
                self.doc[k] = s

            s['_meta']['modified'] = time.time()
            
            self.context.update(self.doc['_id'], {'$set': {k: s}})

            self.doc[k] = s

    def __init__(self, collection, _ids):
        self.collection = collection
        self._ids = _ids
        
        self.bulk = self.collection.initialize_ordered_bulk_op()
        self.empty = True

    def update(self, _id, change):
        self.bulk.find({'_id': _id}).update(change)
        self.empty = False

    def __enter__(self):
        docs = tuple(DocumentContext.Doc(self, self.collection.find_one({'_id':_id})) for _id in self._ids)
        return docs

    def __exit__(self, exc_type, exc_value, tb):
        if exc_type is None:
            if not self.empty:
                self.bulk.execute()



