import subprocess
import os

import pymake

class RuleCSource(pymake.Rule):
    def __init__(self, f_out):
        super(RuleCSource, self).__init__(f_out)

    def f_in(self, makecall):
        yield pymake.ReqFile('build/main.c')

    def build(self, makecall, _, f_in):
        try:
            os.makedirs(os.path.dirname(self.f_out))
        except: pass

        subprocess.call(['gcc', f_in[0].fn, '-o', self.f_out])

def test():
    
    try:
        os.makedirs('build')
    except: pass

    with open('build/main.c', 'w') as f:
        f.write('int main() { return 0; }')
    
    m = pymake.Makefile()
    
    m.rules.append(RuleCSource('build/a.out'))
    
    m.make('build/a.out')

