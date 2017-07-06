import subprocess

import pymake

def touch(filename):
    subprocess.call(['touch',filename])

def test():
    m = pymake.Makefile()

    m.rules['all'] = pymake.Rule('build/a', ['build/b'], lambda fout, fin: touch(fout))
    m.rules['build/b'] = pymake.Rule('build/b', [], lambda fout, fin: touch(fout))
    
    m.make('all')

