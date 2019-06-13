import pickle
import datetime
import logging
import crayons

logger = logging.getLogger(__name__)

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

        if key not in self.d:
            self.d[key] = SubRegistry()

        return self.d[key]

    def __repr__(self):
        if hasattr(self, "doc"):
            if self.doc is not None:
                return f"Sub({self.d}, doc={self.doc})"
            
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
    def __init__(self, doc, d=None):
        self.doc = doc
        self.d = d
        self.mtime = datetime.datetime.now().timestamp()

class DocMeta:
    def __init__(self, d=None):
        self.d = d
        self.mtime = datetime.datetime.now().timestamp()


