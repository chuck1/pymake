import re
from setuptools import setup

with open('pymake/__init__.py') as f:
    version = re.findall("^__version__ = '(.*)'", f.read())[0]

setup(name='pymake',
        version=version,
        description='python makefile system',
        url='http://github.com/chuck1/pymake',
        author='Charles Rymal',
        author_email='charlesrymal@gmail.com',
        license='MIT',
        packages=['pymake'],
        zip_safe=False,
        )

