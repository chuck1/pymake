import os

def makedirs(d):
    #d = os.path.dirname(f)
    try:
        os.makedirs(d)
    except OSError:
        pass

