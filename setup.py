from __future__ import print_function
from setuptools import setup, find_packages
from setuptools.command.test import test as TestCommand
import io
import codecs
import os
import sys
import re

import listwise

here = os.path.abspath(os.path.dirname(__file__))

def read(*filenames, **kwargs):
    encoding = kwargs.get('encoding', 'utf-8')
    sep = kwargs.get('sep', '\n')
    buf = []
    for filename in filenames:
        with io.open(filename, encoding=encoding) as f:
            buf.append(f.read())
    return sep.join(buf)

long_description = read('README.rst', 'CHANGES.rst')

class PyTest(TestCommand):
    def finalize_options(self):
        TestCommand.finalize_options(self)
        self.test_args = []
        self.test_suite = True

    def run_tests(self):
        import pytest
        errcode = pytest.main(self.test_args)
        sys.exit(errcode)
        
setup(
    name='listwise',
    version=listwise.__version__,
    url='https://github.com/zbarge/listwise.git',
    license='Apache Software License',
    author='Zeke Barge',
    tests_require=['pytest'],
    install_requires=['requests>=2.10.0',
                    'sqlite3>=1.0.0',
                    'pandas>=0.18.1',
                    'configparser>=1.0.0'
                    ],
    cmdclass={'test': PyTest},
    author_email='zekebarge@gmail.com',
    description='ListWise.com email validation wrapper.',
    long_description=long_description,
    packages=['listwise'],
    keywords="email validation parsing cleaning cleansing",
    include_package_data=True,
    platforms='any',
    test_suite='listwise.test.test_listwise',
    classifiers = [
        'Programming Language :: Python',
        'Development Status :: 4 - Beta',
        'Natural Language :: English',
        'Environment :: Web Environment',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Apache Software License',
        'Operating System :: OS Independent',
        'Topic :: Software Development :: Libraries :: Python Modules',
        'Topic :: Software Development :: Libraries :: Application Frameworks',
        'Topic :: Internet :: WWW/HTTP :: Dynamic Content',
        ],
    extras_require={
        'testing': ['pytest'],
    }
)