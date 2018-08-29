
def get_subregistry_hashable(r, k):

    if k not in r:
        r[k] = SubRegistry()

    return r[k]

def get_subregistry(r, d):

    assert isinstance(d, dict)

    keys = list(sorted(d.keys()))

    while keys:

        k = keys.pop(0)

        r = get_subregistry_hashable(r, k)

        v = d[k]

        if isinstance(v, dict):
            
            r = get_subregistry(r, v)

        elif isinstance(v, list):
            r = get_subregistry_hashable(r, tuple(v))

        else:
            r = get_subregistry_hashable(r, v)

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

class Doc:
    def __init__(self, doc):
        self.doc = doc
        self.mtime = datetime.datetime.now()

class DocRegistry:

    def __init__(self):
        self._registry = {}

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


