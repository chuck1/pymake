import time
import sys
import logging
import asyncio
import re
import tempfile
import os
import pytest

from mybuiltins import *
import pymake

log = logging.getLogger(__name__)

class A(pymake.RuleRegex):

    pat_out = re.compile('.*/A.txt')

    def build_requirements(self, loop, makecall, func):
        return
        yield

    def build(self, makecall, files_in):

        with makecall.makefile.open(self.f_out, 'w') as f:
            log.info(f'open {self.f_out} write')
            f.write('hello')
            time.sleep(0.5)
            log.info(f'close {self.f_out} write')

class B(pymake.RuleRegex):

    pat_out = re.compile('(.*)/\d+/B.txt')

    def build_requirements(self, loop, makecall, func):
        yield func(pymake.ReqFile(os.path.join(self.groups[0], 'A.txt')))

    def build(self, makecall, files_in):
        f0 = files_in[0].fn


        with makecall.makefile.open(f0) as f:
            log.info(f'open {f0} read')
            a = f.read()
            time.sleep(0.5)
            log.info(f'close {f0} read')

        os.makedirs(os.path.dirname(self.f_out))

        with open(self.f_out, 'w') as f:
            f.write('A.txt: ' + a)

class C(pymake.RuleRegex):

    pat_out = re.compile('(.*)/C.txt')

    def build_requirements(self, loop, makecall, func):
        for i in range(2):
            yield func(pymake.ReqFile(os.path.join(self.groups[0], str(i), 'B.txt')))

    def build(self, makecall, files_in):
        log.info('open C')

        with makecall.makefile.open(self.f_out, 'w') as f:
            f.write('hello')

        log.info('close C')

@pytest.mark.asyncio
async def test(event_loop):

    logging.basicConfig(
        level=logging.INFO,
        format='%(threadName)22s %(name)18s: %(message)s',
        stream=sys.stderr,
        )
    log.level=logging.INFO

    with tempfile.TemporaryDirectory() as d:

        makefile = pymake.Makefile()

        makefile.rules.append(A)
        makefile.rules.append(B)
        makefile.rules.append(C)

        f = os.path.join(d, 'C.txt')

        await makefile.make(event_loop, f)

        assert(os.path.exists(f))

if __name__=='__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(test(loop))




