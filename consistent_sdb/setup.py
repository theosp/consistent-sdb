#!/usr/bin/python
# vim: set fileencoding=utf-8 :

from distutils.core import setup

setup(
    name='consistent_sdb',
    version='1',
    description='Implementation of server consistency layer to aws_simpledb',
    long_description = "Implementation of server consistency layer to aws_simpledb",
    author='Daniel Chcouri',
    author_email='333222@gmail.com',
    url='',

    packages=[
              'consistent_sdb'
             ],
    provides=[
              'consistent_sdb'
             ],
    requires=[
              'aws_simpledb'
             ]
)
