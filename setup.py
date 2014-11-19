#!/usr/bin/env python

# -----------------------------------------------------------------------------
# Copyright (c) 2014--, The biocore Development Team.
#
# Distributed under the terms of the BSD 3-clause License.
#
# The full license is in the file LICENSE, distributed with this software.
# -----------------------------------------------------------------------------

__version__ = "0.1.0-dev"

from setuptools import setup


classes = """
    Development Status :: 4 - Beta
    License :: OSI Approved :: BSD License
    Topic :: Software Development :: Libraries :: Python Modules
    Programming Language :: Python
    Programming Language :: Python :: 2.7
    Programming Language :: Python :: Implementation :: CPython
    Operating System :: OS Independent
    Operating System :: POSIX :: Linux
    Operating System :: MacOS :: MacOS X
"""

long_description = """GD: Easy SQL connection handling"""

classifiers = [s.strip() for s in classes.split('\n') if s]

setup(name='gd',
      version=__version__,
      long_description=long_description,
      license="BSD",
      description='SQL connection handler',
      author="Biocore development team",
      author_email="josenavasmolina@gmail.com",
      url='http://github.com/biocore/glowing-dangerzone.git',
      test_suite='nose.collector',
      packages=['gd'],
      package_data={'gd': ['support_files/config.txt']},
      extras_require={'test': ["nose >= 0.10.1", "pep8", 'flake8']},
      install_requires=['psycopg2', 'future==0.13.0'],
      classifiers=classifiers
      )
