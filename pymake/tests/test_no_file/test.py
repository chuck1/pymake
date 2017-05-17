import termcolor

import pymake

class A(pymake.Rule):
    def __init__(self,b):
        super(A,self).__init__(self.f_out, self.f_in, self.build)

        self.b = b

    def f_out(self):
        yield 'A.txt'

    def f_in(self, makefile, test, force):
        yield self.b

    def build(self, f_out, f_in):
        print(termcolor.colored('build A','yellow'))
        open('A.txt','w').write('hello')
        return 0

class B(pymake.Rule):
    def __init__(self):
        super(B,self).__init__(self.f_out, self.f_in, self.build)

    def f_out(self):
        yield 'B'

    def f_in(self, makefile, test, force):
        yield 'C.txt'

    def build(self, f_out, f_in):
        print(termcolor.colored('build B','yellow'))
        return 0

def test():
    
    b = B()
    a = A(b)

    m = pymake.Makefile()

    m.rules += [a,b]

    m.make(a,test=True)

if __name__ == '__main__':
    test()




