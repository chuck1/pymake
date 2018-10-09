import datetime
import logging
import os
import pickle
import shelve

import crayons
import pymake.doc_registry.address

logger = logging.getLogger(__name__)


def get_subregistry(r, l, f=None):

    assert isinstance(l, list)

    assert isinstance(r, (shelve.Shelf, SubRegistry))

    for k in l:

        if k not in r:
            r[k] = SubRegistry()
        
        r = r[k]

        # created for debugging
        if f is not None: f(r)

        assert isinstance(r, SubRegistry)

    return r
 
class SubRegistry:
    def __init__(self):
        self.d = {}
        self.doc = None

    def items(self):
        return self.d.items()

    def __iter__(self):
        return iter(self.d)

    def __setitem__(self, key, value):
        self.d[key] = value

    def __getitem__(self, key):
        return self.d[key]

    def __str__(self):
        return str(self.d)

    def __repr__(self):
        if self.doc is not None:
            return f"Sub({self.d}, doc={self.doc})"
        else:
            return f"Sub({self.d})"

    def __getstate__(self):
        state = dict(self.__dict__)
        if "doc" in state:
            if state["doc"] is not None:
                try:
                    pickle.dumps(state["doc"])
                except Exception as e:
                    logger.warning(crayons.yellow(f'error pickling {state["doc"]} {e!r}'))
                    #logger.warning(crayons.yellow(repr(e)))
                    del state["doc"]
        return state

class Doc:
    def __init__(self, doc):
        self.doc = doc
        self.mtime = datetime.datetime.now().timestamp()

class DocRegistry:
    def __init__(self, db):
        #if os.path.exists("build/doc_registry.bin"):
        #    with open("build/doc_registry.bin", "rb") as f:
        #        self._registry = pickle.load(f)
        #else:
        #    self._registry = SubRegistry()

        self._registry = db

        # verify that registry can be pickled
        if __debug__:
            pass
            #pickle.dumps(self._registry)

        # for debugging
        self.keys_read = set()

    def delete(self, d):
        logger.warning(crayons.yellow("delete"))

        r = self.get_subregistry(d)
        
        r.doc = None

    def get_subregistry(self, d, f=None):
        a = pymake.doc_registry.address.Address(d)
        r = self._registry
        r = get_subregistry(r, a.l, f=f)
        return r

    def exists(self, d):
        r = self.get_subregistry(d)
        if not hasattr(r, "doc"): return False
        return (r.doc is not None)

    def read(self, d):

        #a = pymake.doc_registry.address.Address(d)

        #self.keys_read.add(a.l[0])

        r = self.get_subregistry(d)

        logger.debug(f"read from {type(r)} {id(r)}")

        if r.doc is None:
            raise Exception(f"Object not found: {d!r}")

        return r.doc.doc

    def read_mtime(self, d):

        r = self.get_subregistry(d)

        return r.doc.mtime

    def write(self, d, doc):

        # verify that registry can be pickled before addition
        #if __debug__: pickle.dumps(self._registry)

        r = self.get_subregistry(d)

        r.doc = Doc(doc)

        if False:#__debug__:
            try:
                s = len(pickle.dumps(self._registry))
            except Exception as e:
                logger.error(crayons.red(f"addition of {d!r} {doc!r} caused pickle to fail"))
                raise
    
            logger.info(f"registry size: {s}")

    def test_pickle(self, l, r):
        if hasattr(r, "doc"):
            if r.doc:
                try:
                    pickle.dumps(r.doc)
                except Exception as e:
                    logger.error(repr(l))
                    logger.error(repr(r.doc))
                    logger.error(repr(e))
    
                    for k, v in r.doc.__dict__.items():
                        try:
                            pickle.dumps(v)
                        except Exception as e:
                            logger.error(repr(k))
                            logger.error(repr(v))
                            logger.error(repr(e))

        for k, v in r.d.items():
            self.test_pickle(l + [k], v)

    def dump(self):
        try:
            s = pickle.dumps(self._registry)
        except:
            self.test_pickle([], self._registry)
            raise

        with open("build/doc_registry.bin", "wb") as f:
            f.write(s)
       
        



