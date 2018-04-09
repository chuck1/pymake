import re
import tempfile
import os

import pytest

import pymake

class A(pymake.RuleRegex):

    pat_out = re.compile('.*/A.txt')

    def build_requirements(self, makecall, func):
        return
        yield

    def build(self, makecall, files_in):
        with open(self.f_out, 'w') as f:
            f.write('hello')

@pytest.mark.asyncio
async def test():
    with tempfile.TemporaryDirectory() as d:

        makefile = pymake.Makefile()

        makefile.rules.append(A)

        f = os.path.join(d, 'A.txt')

        await makefile.make(f)

        assert(os.path.exists(f))

