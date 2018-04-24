import hashlib
import json
import base64
import re
import tempfile
import os
import contextlib
import pprint
import crayons
import logging

logger = logging.getLogger(__name__)

def breakpoint(): import pdb; pdb.set_trace();

def normalized_list(a):
    return [normalized(i) for i in a]

def normalized_dict(a):
    return dict((k, normalized(a[k])) for k in sorted(a.keys()))

def normalized(a):
    if isinstance(a, dict):
        return normalized_dict(a)
    elif isinstance(a, list):
        return normalized_list(a)
    else:
        return a

def get_hash(desc):
    # desc is a json compatible object

    desc = normalized(desc)
    
    b = json.dumps(desc, sort_keys=True).encode()

    h = hashlib.md5(b)

    s = h.hexdigest()

    return s

# the find index is a dict in which the keys are hashes of file descriptors
# under each key is a dict of file names and descriptors

INDEX_DIR = "data/.file_index"

class IndexFile:
    def __init__(self, h):
        self.h = h

    def __enter__(self):

        try:
            os.makedirs(INDEX_DIR)
        except OSError:
            pass

        if not os.path.exists(os.path.join(INDEX_DIR, self.h)):
            with open(os.path.join(INDEX_DIR, self.h), 'w') as f:
                json.dump({}, f)

        with open(os.path.join(INDEX_DIR, self.h)) as f:
            s = f.read()

        try:
            self.index = json.loads(s)
        except:
            logger.error(f'could not json decode: {s!r}')

            self.index = {}

        return self

    def __exit__(self, exc_type, exc_value, traceback):
        with open(os.path.join(INDEX_DIR, self.h), 'w') as f:
            json.dump(self.index, f)

    def sub_index(self, H):
        
        sub = self.index

        for h in H:
            if h not in sub:
                sub[h] = {}

            sub = sub[h]

        return sub
    
class Manager:
    def __init__(self):
        pass

    def split_hash(self, h):
        h0 = h[:2]
        h1 = h[2:16]
        h2 = h[16:]
        return h0, h1, h2

    def _get_hash(self, desc):

        h = get_hash(desc)
        H = self.split_hash(h)

        #print(crayons.magenta(f'get_hash {h} {json.dumps(desc, sort_keys=True)}', bold=(h[:3]=='dc7') or (h[:3]=='fd3')))

        with IndexFile(H[0]) as index:

            sub_index = index.sub_index(H[1:-1])

            if H[-1] in sub_index:
                if sub_index[H[-1]] != desc:
                    breakpoint()
    
                assert sub_index[H[-1]] == desc
    
            else:
                sub_index[H[-1]] = desc
 
                assert sub_index[H[-1]] == desc
                assert get_hash(sub_index[H[-1]]) == h

            return h

    def get_filename(self, desc):
        h = self._get_hash(desc)
        
        h0, h1, h2 = self.split_hash(h)

        f = f"build/data/index/{h0}/{h1}/{h2}"

        try:
            os.makedirs(os.path.dirname(f))
        except OSError: pass

        return f

    def get_descriptor(self, h0, h1, h2):
        #h0, h1, h2 = self.split_hash(h)
        with IndexFile(h0) as index:
            return index.index[h1][h2]

    def get_descriptor_from_filename(self, s):
        m = re.match(('build/data/index/'
            '([0-9a-f]{2})/'
            '([0-9a-f]{14})/'
            '([0-9a-f]{16})'
            ), s)
        return self.get_descriptor(m.group(1), m.group(2), m.group(3))

manager = Manager()

def test():
    d = {
            'a': [
                {
                    'a': 1,
                    },
                ],
            'b': 3.1415,
            }

    print(d)

    s = get_hash(d)

    print(s)

    print(manager.get_filename(d))


if __name__ == '__main__':
    test()

    

