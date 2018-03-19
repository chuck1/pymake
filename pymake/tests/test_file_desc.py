import os
import re
import tempfile

import pymake

class Rule(pymake.RuleFileDescriptor):
    @classmethod
    def descriptor_pattern(cls):
        return {'a': 1}

    def f_in(self, mc):
        return
        yield

    def build(self, makecall, _1, _2):
        print(self.f_out)

def test_1():
        
    m = pymake.Makefile()
        
    m.rules.append(Rule)
    
    m.make(pymake.ReqFileDescriptor({'a': 1}, '.txt'))


