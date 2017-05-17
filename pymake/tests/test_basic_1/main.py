import pymake

class MyRule(pymake.Rule):
    def __init__(self):
        pymake.Rule.__init__(self, self.f_out, self.f_in, self.build)
    def f_out(self):
        yield 'b.txt'
    def f_in(self, makecall):
        yield 'a.txt'
    def build(self, f_out, f_in):
        open(f_out[0], 'w').write('input file contents:\n' + open(f_in[0], 'r').read())

m = pymake.Makefile()
m.rules.append(MyRule())
m.make('b.txt')

