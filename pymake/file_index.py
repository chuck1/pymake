import hashlib
import json
import base64
import tempfile
import os
import contextlib
import pprint

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

    h = hashlib.md5(json.dumps(desc).encode())

    s = h.hexdigest()

    return s

# the find index is a dict in which the keys are hashes of file descriptors
# under each key is a dict of file names and descriptors

INDEX_FILENAME = ".file_index.json"

class Manager:
    def __init__(self):

        if not os.path.exists(INDEX_FILENAME):
            with open(INDEX_FILENAME, 'w') as f:
                f.write(json.dumps({'next_int': 0, 'folders':{}}))

        with open(INDEX_FILENAME) as f:
            self.index = json.load(f)
 
    def write(self):
        with open(INDEX_FILENAME, "w") as f:
            f.write(json.dumps(self.index, indent=4))

    def next_int(self):
        i = self.index["next_int"]
        self.index["next_int"] += 1
        self.write()
        return i
   
    def get_folder(self, h):
        if h not in self.index["folders"]:
            self.index["folders"][h] = {}

        return self.index["folders"][h]

    def get_filename_1(self, desc):
        
        h = get_hash(desc)
       
        folder = self.get_folder(h)

        for i, d in folder.items():
            if d == desc:
                return h, i
        
        i = self.next_int()
        folder[i] = desc
        self.write()

        return h, i

    def get_filename(self, desc):
        h, i = self.get_filename_1(desc)

        f = f"data/index/{h}/{i}"

        try:
            os.makedirs(os.path.dirname(f))
        except OSError: pass

        return f

    def get_descriptor(self, folder, i):
        f = self.index['folders'][folder]
        if i not in f:
            pprint.pprint(f)
        d = f[i]
        return d

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

    print(manager.get_filename(d, '.txt'))


if __name__ == '__main__':
    test()

    

