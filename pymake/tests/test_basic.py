import subprocess

import pymake

def touch(filename):
    subprocess.call(['touch',filename])

class RuleA(pymake.Rule):
    def __init__(self):
        super(RuleA, self).__init__('build/a')

    def f_in(self, makecall):
        yield pymake.ReqFile('build/b')

    def build(self, makecall, _, f_in):
        touch(self.f_out)

class RuleB(pymake.Rule):
    def __init__(self):
        super(RuleB, self).__init__('build/b')

    def build(self, makecall, _, f_in):
        touch(self.f_out)

def test():
    m = pymake.Makefile()

    m.rules.append(RuleA())
    m.rules.append(RuleB())
    
    m.make('build/a')

