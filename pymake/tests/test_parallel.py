
import pytest

import pymake

class A(pymake.rules.RuleDoc):
    @classmethod
    def descriptor_pattern(cls):
        return {
                "type_": "A",
                }

@pytest.mark.asyncio
async def test_parallel_0(makefile):

    pymake.rules.USE_TASKS = True

    mc = pymake.makecall.MakeCall()



