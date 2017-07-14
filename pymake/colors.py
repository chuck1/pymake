import functools
import crayons

def _colored(crayon, s, print_=print, **kwargs):
    print_(crayon(s, **kwargs))

red = functools.partial(_colored, crayons.red, bold=True)
yellow = functools.partial(_colored, crayons.yellow, bold=True)
green = functools.partial(_colored, crayons.green, bold=True)
blue = functools.partial(_colored, crayons.blue, bold=True)
magenta = functools.partial(_colored, crayons.magenta, bold=True)

