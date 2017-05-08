
# PyMake

PyMake is a Python replacement for the linux make system.

# Example

To use PyMake, create a subclass of pymake.Rule.
The init function of pymake.Rule takes three callable objects.
The first two callables should return generators which produce the output and input files.
The third callable should generate the outputs from the inputs.

    class MyRule(pymake.Rule):
        def __init__(self):
            pymake.Rule.__init__(self, self.f_out, self.f_in, self.build)
        def f_out(self):
            yield 'b.txt'
        def f_in(self, makecall):
            yield 'a.txt'
        def build(self, f_out, f_in):
            open(f_out[0], 'w').write('input file contents:\n' + open(f_in[0], 'r').read())

To use our rule, we must create a pymake.Makefile object and add our rule to it.

    m = pymake.Makefile()
    m.rules.append(MyRule())
    m.make('b.txt')

If we run the above code without a file named a.txt in the working directory, we will get an exception

    Exception: no rules to make a.txt

You will find this code in test/test_basic_1.

