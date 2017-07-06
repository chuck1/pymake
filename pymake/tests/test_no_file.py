import termcolor

import pymake

class A(pymake.Rule):
    def __init__(self, b):
        super(A,self).__init__('build/A.txt')
        self.b = b

    def f_in(self, makecall):
        yield self.b

    def build(self, makecall, _, f_in):
        print(termcolor.colored('build A','yellow'))
        open(self.f_out, 'w').write('hello')

class B(pymake.Rule):
    def __init__(self):
        super(B, self).__init__('build/B')

    def f_in(self, makecall):
        yield pymake.ReqFile('build/C.txt')

    def build(self, makecall, _, f_in):
        print(termcolor.colored('build B','yellow'))

def test():

    with open('build/C.txt', 'w') as f:
        f.write('')

    b = B()
    a = A(b)

    m = pymake.Makefile()

    m.rules += [a,b]

    m.make(a, test=True)





