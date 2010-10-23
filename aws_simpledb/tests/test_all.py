#!/usr/bin/python
# vim: set fileencoding=utf-8 :

"""This module runs all the aws_simpledb tests
"""

import unittest
import helpers

test_modules = ['test_aws_simpledb_domains_actions', 'test_actions']

modules_suites = []
for module in test_modules:
    module = __import__(module)
    modules_suites.append(unittest.TestLoader().loadTestsFromModule(module))

all_tests = unittest.TestSuite(modules_suites)
unittest.TextTestRunner(verbosity=2).run(all_tests)
