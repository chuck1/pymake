import datetime
import logging
import os
import pickle

import crayons

logger = logging.getLogger(__name__)

def get_subregistry_hashable(r, k):

    if k not in r:
        r[k] = SubRegistry()

    return r[k]

def get_subregistry_1(r, v):
    
    if isinstance(v, dict):
        return get_subregistry(r, v)

    elif isinstance(v, list):
        return get_subregistry_list(r, v)

    else:
        return get_subregistry_hashable(r, v)
       
def get_subregistry_list(r, l):

    r = get_subregistry_hashable(r, "$LIST")

    for v in l:
        r = get_subregistry_hashable(r, "$LISTELEMENT")

        r = get_subregistry_1(r, v)

    return r

def get_subregistry(r, d):

    assert isinstance(d, dict)

    keys = list(sorted(d.keys()))

    assert isinstance(r, SubRegistry)

    while keys:

        k = keys.pop(0)

        r = get_subregistry_hashable(r, k)

        assert isinstance(r, SubRegistry)

        v = d[k]

        r = get_subregistry_1(r, v)

        assert isinstance(r, SubRegistry)

    return r
 
class SubRegistry:
    def __init__(self):
        self.d = {}
        self.doc = None

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
                    logger.warning(crayons.yellow(f'error pickling {state["doc"]}'))
                    logger.warning(crayons.yellow(repr(e)))
                    del state["doc"]
        return state

class Doc:
    def __init__(self, doc):
        self.doc = doc
        self.mtime = datetime.datetime.now().timestamp()

class DocRegistry:
    def __init__(self):
        if os.path.exists("build/doc_registry.bin"):
            with open("build/doc_registry.bin", "rb") as f:
                self._registry = pickle.load(f)
        else:
            self._registry = SubRegistry()

    def read(self, d):

        r = self._registry

        r = get_subregistry(r, d)

        return r.doc.doc

    def read_mtime(self, d):

        r = self._registry

        r = get_subregistry(r, d)

        return r.doc.mtime

    def write(self, d, doc):

        r = self._registry

        r = get_subregistry(r, d)

        r.doc = Doc(doc)

        logger.info("registry size: {}".format(len(pickle.dumps(self._registry))))

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
       
        



