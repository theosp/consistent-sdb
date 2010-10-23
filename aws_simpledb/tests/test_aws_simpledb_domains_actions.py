#!/usr/bin/python
# vim: set fileencoding=utf-8 :

"""The ability to create and delete domains is essential to all the actions
test so this tests were seperated from the regular actions test case.

"""

import unittest

import startup
import settings
import helpers

import aws_simpledb

# For this tests we assume has_domain works fine
class TC00_CreateDomain(unittest.TestCase):
    def setUp(self):
        self.connection = aws_simpledb.SimpleDB()
        # If the testing domain, already exists, delete it
        if self.connection.has_domain(settings.test_domains[0]):
            helpers.msg(self, 'Test domain already exists - delete it')
            self.connection.delete_domain(settings.test_domains[0])
            helpers.eventualConsistencySleep()

    def tearDown(self):
        self.connection.delete_domain(settings.test_domains[0])

    def runTest(self):
        self.connection.create_domain(settings.test_domains[0])
        helpers.msg(self, 'Test Domain created')
        helpers.eventualConsistencySleep()

        self.assertTrue(self.connection.has_domain(settings.test_domains[0]))

class TC01_DeleteDomain(unittest.TestCase):
    def setUp(self):
        self.connection = aws_simpledb.SimpleDB()
        if not self.connection.has_domain(settings.test_domains[0]):
            helpers.msg(self, 'Create test domain')
            self.connection.create_domain(settings.test_domains[0])
            helpers.eventualConsistencySleep()

    def runTest(self):
        self.connection.delete_domain(settings.test_domains[0])
        self.assertTrue(not self.connection.has_domain(settings.test_domains[0]))

if __name__ == "__main__":
    unittest.main()
