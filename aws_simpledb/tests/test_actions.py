#!/usr/bin/python
# vim: set fileencoding=utf-8 :

import unittest

import inspect

import startup
import settings
import helpers

import aws_simpledb

from orderedset import OrderedSet

class TestAwsSimpledbActions(unittest.TestCase):
    """Tests the aws simpledb api actions implemented in aws_simpledb.

    Since all the actios below rely on create_domain, the create_domain test
    was seperated to the test_create_domain module.
    """

    def setUp(self):
        self.connection = aws_simpledb.SimpleDB()
        self.test_items = ['test_item1', 'test_item2']

        # If the testing domains doesn't exist - create them
        for domain in settings.test_domains:
            if not self.connection.has_domain(domain):
                self.connection.create_domain(domain)

        for domain in settings.test_domains:
            for item_name in self.test_items:
                self.connection.delete_attributes(domain, item_name)

    def tearDown(self):
        tests_methods = \
            [method for method in dir(self) if 
             inspect.ismethod(getattr(self, method)) and method[0:4] == 'test']
        if tests_methods[-1] == self.id().split('.')[-1]:
            for domain in settings.test_domains:
                if self.connection.has_domain(domain):
                    helpers.msg(self, 'Deleting test domain ' + domain)
                    self.connection.delete_domain(domain)

    def test_00_put_attributes(self):
        # new attributes, some with more with one value
        self.connection.put_attributes(
         settings.test_domains[0],
         self.test_items[0],
         {
          'a': {
                'values': ['1', '2'],
                'replace': False
               },
          'b': {
                'values': ['3'],
                'replace': False
               },
          'c': {
                'values': ['4'],
                'replace': False
               }
         }
        )

        # append attributes
        self.connection.put_attributes(
                                  settings.test_domains[0],
                                  self.test_items[0],
                                  {
                                   'b': {
                                         'values': ['2'],
                                         'replace': False 
                                        }
                                  }
                                 )

        # override attributes
        self.connection.put_attributes(
                                  settings.test_domains[0],
                                  self.test_items[0],
                                  {
                                   'c': {
                                         'values': ['3'],
                                         'replace': True 
                                        }
                                  }
                                 )
        
        helpers.eventualConsistencySleep()
 
        self.assertEqual(
            {
                'a': OrderedSet(['1', '2']),
                'b': OrderedSet(['2', '3']),
                'c': OrderedSet(['3'])
            },
            self.connection.get_attributes(settings.test_domains[0], self.test_items[0])
        )

    def test_01_batch_put_attributes(self):
        # new attributes, some with more with one value
        self.connection.batch_put_attributes(
         settings.test_domains[0],
         {
          self.test_items[0]:
          {
           'a': {
                 'values': ['1', '2'],
                 'replace': False
                },
           'b': {
                 'values': ['3'],
                 'replace': False
                }
          },
          self.test_items[1]:
          {
           'a': {
                 'values': ['3', '5'],
                 'replace': False
                },
           'b': {
                 'values': ['7'],
                 'replace': False
                },
          }
         }
        )

        helpers.eventualConsistencySleep()

        self.assertEqual(
         self.connection.get_attributes(
          settings.test_domains[0],
          self.test_items[0]
         ),
         {'a': OrderedSet(['1','2']), 'b': OrderedSet(['3'])}
        )

        self.assertEqual(
         self.connection.get_attributes(
          settings.test_domains[0],
          self.test_items[1]
         ),
         {'a': OrderedSet(['3', '5']), 'b': OrderedSet(['7'])}
        )

    def test_02_get_attributes(self):
        self.connection.put_attributes(
                                  settings.test_domains[0],
                                  self.test_items[0],
                                  {
                                   'a': {
                                         'values': ['0'],
                                         'replace': False
                                        },
                                   'b': {
                                         'values': ['2'],
                                         'replace': False
                                        },
                                   'c': {
                                         'values': ['1', '2'],
                                         'replace': False
                                        }
                                  }
                                 )

        helpers.eventualConsistencySleep()

        # Get entire item
        self.assertEqual(
                         self.connection.get_attributes(
                          settings.test_domains[0],
                          self.test_items[0]
                         ),
                         {'a': OrderedSet(['0']), 'b': OrderedSet(['2']), 'c': OrderedSet(['1', '2'])}
                        )

        # Get specific Attributes
        self.assertEqual(
                         self.connection.get_attributes(
                          settings.test_domains[0],
                          self.test_items[0],
                          ['a', 'b']
                         ),
                         {'a': OrderedSet(['0']), 'b': OrderedSet(['2'])}
                        )

        # Get attribute which has more than one value
        self.assertEqual(
                         self.connection.get_attributes(
                          settings.test_domains[0],
                          self.test_items[0],
                          ['c']
                         ),
                         {'c': OrderedSet(['1', '2'])}
                        )

        # Get attribute that doesn't exists
        # When requesting all the Item
        self.assertTrue(
            not self.connection.get_attributes(settings.test_domains[0], self.test_items[0]).has_key('d')
        )

        # When requesting it specifically
        self.assertEqual(
            self.connection.get_attributes(
             settings.test_domains[0],
             self.test_items[0],
             ['d']
            ),
            {'d': OrderedSet([])}
        )

        # Get attribute that doesn't exists from item that doesn't exists
        self.assertEqual(
                         self.connection.get_attributes(
                          settings.test_domains[0],
                          'test_item2',
                          ['d']
                         ),
                         {'d': OrderedSet([])}
        )

        # Get item that doesn't exists
        self.assertEqual(
                         self.connection.get_attributes(
                          settings.test_domains[0],
                          'test_item2'
                         ),
                         {}
        )

    def test_03_delete_attributes(self):
        self.connection.put_attributes(
                                  settings.test_domains[0],
                                  self.test_items[0],
                                  {
                                   'a': {
                                         'values': ['0'],
                                         'replace': False
                                        },
                                   'b': {
                                         'values': ['1', '2'],
                                         'replace': False
                                        },
                                   'c': {
                                         'values': ['1', '2', '3'],
                                         'replace': False
                                        },
                                   'd': {
                                         'values': ['1', '2'],
                                         'replace': False
                                        },
                                   'e': {
                                         'values': ['a', 'b', 'c'],
                                         'replace': False
                                        },
                                   'f': {
                                         'values': ['a', 'b', 'c'],
                                         'replace': False
                                        },
                                   'g': {
                                         'values': ['go'],
                                         'replace': False
                                        },
                                   'h': {
                                         'values': ['foo'],
                                         'replace': False
                                        },
                                  }
                                 )

        # Delete specific attribute's values
        self.connection.delete_attributes(settings.test_domains[0], self.test_items[0], {'c': ['2', '3']})
        
        # Delete specific attribute's value
        self.connection.delete_attributes(settings.test_domains[0], self.test_items[0], {'d': '2'})

        # Delete all attribute's values
        self.connection.delete_attributes(settings.test_domains[0], self.test_items[0], {'b': OrderedSet()})

        # Delete specific values from more than one attribute
        self.connection.delete_attributes(settings.test_domains[0], self.test_items[0], {'a': '0', 'e': ['a', 'b'], 'f': ['c']})

        # Delete more than one attribute
        self.connection.delete_attributes(settings.test_domains[0], self.test_items[0], {'g': None, 'h': None})

        helpers.eventualConsistencySleep()

        # Check deletion of specific attribute's values
        self.assertEqual(
            self.connection.get_attributes(settings.test_domains[0], self.test_items[0], ['c']),
            {'c': OrderedSet(['1'])}
        )

        # Check deletion of specific attribute's value
        self.assertEqual(
            self.connection.get_attributes(settings.test_domains[0], self.test_items[0], ['d']),
            {'d': OrderedSet(['1'])}
        )

        # Check deletion of all attribute's values
        self.assertEqual(
            self.connection.get_attributes(settings.test_domains[0], self.test_items[0], ['b']),
            {'b': OrderedSet([])}
        )

        # Check deletion of specific values from more than one attribute
        self.assertEqual(
            self.connection.get_attributes(settings.test_domains[0], self.test_items[0], ['a', 'e', 'f']),
            {'a': OrderedSet([]), 'e': OrderedSet(['c']), 'f': OrderedSet(['a', 'b'])}
        )

        # Check deletion of more than one attribute
        self.assertEqual(
            self.connection.get_attributes(settings.test_domains[0], self.test_items[0], ['g', 'h']),
            {'g': OrderedSet([]), 'h': OrderedSet([])}
        )

        # Delete entire item
        self.connection.delete_attributes(settings.test_domains[0], self.test_items[0])

        helpers.eventualConsistencySleep()

        # Check deletion of entire item
        self.assertEqual(
            self.connection.get_attributes(settings.test_domains[0], self.test_items[0]),
            {}
        )

    def test_04_select(self):
        self.connection.delete_attributes(settings.test_domains[0], self.test_items[0])
        self.connection.delete_attributes(settings.test_domains[0], self.test_items[1])
        
        # new attributes, some with more with one value
        self.connection.batch_put_attributes(
                                  settings.test_domains[0],
                                  {
                                   self.test_items[0]:
                                   {
                                    'a': {
                                          'values': ['1', '2'],
                                          'replace': False
                                         },
                                    'b': {
                                          'values': ['3'],
                                          'replace': False
                                         }
                                   },
                                   self.test_items[1]:
                                   {
                                    'a': {
                                          'values': ['3', '5'],
                                          'replace': False
                                         },
                                    'b': {
                                          'values': ['7'],
                                          'replace': False
                                         },
                                   }
                                  }
                                 )
 
        helpers.eventualConsistencySleep()

        print self.connection.get_attributes(settings.test_domains[0], self.test_items[0]),
        print self.connection.get_attributes(settings.test_domains[0], self.test_items[1]),

        print self.connection.select('a', settings.test_domains[0], 'a > "1"')

if __name__ == "__main__":
    unittest.main()
