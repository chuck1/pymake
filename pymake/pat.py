import re


class Pat:
    pass

class PatString(Pat):
    def __init__(self, pattern_string):
        self.pat = re.compile(pattern_string)

    def match(self, s):
        return bool(self.pat.match(s))

class PatInt(Pat):
    def __init__(self, start=None, stop=None):
        self.start = start
        self.stop = stop

    def match(self, i):
        try:
            i = int(i)
        except:
            return False

        if self.start is not None:
            if i < self.start:
                return False
            
        if self.stop is not None:
            if i >= self.stop:
                return False

        return True

class PatFloat(Pat):
    def match(self, x):
        try:
            float(x)
            return True
        except:
            return False

class PatDict(Pat):
    def match(self, x):
        return isinstance(x, dict)


class PatNullable(Pat):
    def __init__(self, pat):
        self.pat = pat

    def match(self, thing):
        return self.pat.match(thing)




