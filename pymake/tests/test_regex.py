import re
import tempfile
import os

import pymake



class A(pymake.RuleRegex):

    pat_out = re.compile('.*/A.txt')

    def build_requirements(self, makecall, func):
        return
        yield

    def build(self, makecall, files_in):
        with open(self.f_out, 'w') as f:
            f.write('hello')

def test():
    with tempfile.TemporaryDirectory() as d:

        makefile = pymake.Makefile()

        makefile.rules.append(A)
    

        f = os.path.join(d, 'A.txt')

        makefile.make(f)

        assert(os.path.exists(f))

