import asyncio
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

class B(pymake.RuleRegex):

    pat_out = re.compile('(.*)/B.txt')

    def build_requirements(self, makecall, func):
        yield func(pymake.ReqFile(os.path.join(self.groups[0], 'A.txt')))

    def build(self, makecall, files_in):
        with open(self.f_out, 'w') as f:
            f.write('hello')

@pytest.mark.asyncio
async def test():

    loop = asyncio.get_event_loop()

    with tempfile.TemporaryDirectory() as d:

        makefile = pymake.Makefile()

        makefile.rules.append(A)
        makefile.rules.append(B)

        f = os.path.join(d, 'B.txt')

        await makefile.make(loop, f)

        assert(os.path.exists(f))

