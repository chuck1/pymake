import pymongo
import pymake

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

