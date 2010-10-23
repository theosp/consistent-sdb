#!/usr/bin/python
# vim: set fileencoding=utf-8 :

from distutils.core import setup

setup(
    name='aws_simpledb',
    version='1',
    description='Python library for accessing Amazon SimpleDB API',
    long_description = "Python library for accessing Amazon SimpleDB API",
    author='Daniel Chcouri',
    author_email='333222@gmail.com',
    url='',
    # Note: The code is a fork of the sixapart's python-simpledb:
    # http://github.com/sixapart/python-simpledb

    packages=[
              'aws_simpledb'
             ],
    provides=[
              'aws_simpledb'
             ],
    requires=[
              'orderedset'
             ]
)
