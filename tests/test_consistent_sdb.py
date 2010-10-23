#!/usr/bin/python
# vim: set fileencoding=utf-8 :

import helpers
import settings
import startup

import consistent_sdb.settings

import unittest

import consistent_sdb

import inspect
import pickle
import copy

from orderedset import OrderedSet

import datetime

import httplib2

class TestConsistentSdb(unittest.TestCase):
    def setUp(self):
        self.connection = consistent_sdb.connection

        self.test_items = ['test_item1', 'test_item2', 'test_item3']

        # If the testing domains doesn't exist - create them
        print 'Checking test domains exist'
        for domain in settings.test_domains:
            if not self.connection.has_domain(domain):
                self.connection.create_domain(domain)

        # Initiate items
        print 'Initiating test items'
        for domain in settings.test_domains:
            for item_name in self.test_items:
                self.connection.delete_attributes(domain, item_name)

    def tearDown(self):
        # For the last check we also, delete the testing domains
        # note that if you run only some of this TestCase's tests it's up to
        # you to delete the test domains.
        tests_methods = \
            [method for method in dir(self) if 
             inspect.ismethod(getattr(self, method)) and method[0:4] == 'test']
        if tests_methods[-1] == self.id().split('.')[-1]:
            print 'Deleting test domains'
            for domain in settings.test_domains:
                if self.connection.has_domain(domain):
                    helpers.msg(self, 'Deleting test domain ' + domain)
                    self.connection.delete_domain(domain)

    def test_00_log_action(self):
        timestamp = consistent_sdb.current_timestamp()

        actions_journal_key = \
            consistent_sdb.actions_journal_key(settings.test_domains[0], self.test_items[0])
        action_log_key = \
            consistent_sdb.action_log_key(settings.test_domains[0], self.test_items[0], timestamp)

        # init the actions journal before we begin
        consistent_sdb.journals_db.delete(actions_journal_key)

        action = 'delete'
        attributes = {'attribute_0':
                      {
                       '0': ['value_0', 'value_1']
                      }
                     }


        consistent_sdb.log_action(settings.test_domains[0], self.test_items[0], timestamp, action, attributes)

        # test write to the actions_journal_key
        self.assertEqual(
                         consistent_sdb.journals_db.llen(actions_journal_key),
                         1
                        )

        self.assertEqual(
                         consistent_sdb.journals_db.lrange(actions_journal_key, 0, 1),
                         [timestamp]
                        )
        
        # test creation of action_log_key
        self.assertEqual(
                         consistent_sdb.logs_db.get(action_log_key),
                         pickle.dumps({'action': action, 'attributes': attributes})
                        )

        # test expiry time set
        self.assertTrue(
                        consistent_sdb.logs_db.ttl(action_log_key) > 0
                       )

    def test_01_delete(self):
        domain_a_new_items = \
            {
             self.test_items[0]: {
                               'a': {'values': ['0', '1', '2'],
                                     'replace': False
                                    },
                              },

             self.test_items[1]: {      
                               'b': {'values': ['3', '4', '5'],
                                     'replace': False
                                    },
                               'c': {'values': ['6', '7', '8'],
                                     'replace': False
                                    },
                              },

             self.test_items[2]: {
                               'd': {'values': ['6', '7', '8'],
                                     'replace': False
                                    },
                               'e': {'values': ['9', '10', '11'],
                                     'replace': False
                                    },
                              }
            }

        domain_b_new_items = \
            {
             self.test_items[0]: {
                               'f': {'values': ['10', '11', '12'],
                                     'replace': False
                                    },
                              },
            }

        consistent_sdb.connection.batch_put_attributes(settings.test_domains[0], domain_a_new_items)
        consistent_sdb.connection.batch_put_attributes(settings.test_domains[1], domain_b_new_items)

        delete_records = {
                          settings.test_domains[0]: {
                            # delete only some values from item1's attribute
                            # one of the values doesn't exists on purpose
                            self.test_items[0]: {
                                              'a': ['0', '3'],  
                                             },
                            # delete item2's attribute totally
                            self.test_items[1]: {
                                              'b': [],
                                             },
                            # delete item3
                            self.test_items[2]: {}
                          },
                          settings.test_domains[1]: {
                            # delete some values from domain_b's item1 attribute
                            # important to see if deleting from more than one
                            # domain works
                            self.test_items[0]: {
                                              'f': ['10', '11'],
                                             }
                          }
                         }

        consistent_sdb.delete(delete_records)

        helpers.eventualConsistencySleep()

        consistent_sdb.connection.http = httplib2.Http(timeout=10)

        d1_item1 = \
            consistent_sdb.connection.get_attributes(
                                               settings.test_domains[0],
                                               self.test_items[0]
                                              )
        d1_item1_no_change_timestamp = copy.deepcopy(d1_item1)

        d1_item2 = d1_item2_no_change_timestamp = \
            consistent_sdb.connection.get_attributes(
                                               settings.test_domains[0],
                                               self.test_items[1]
                                              )
        d1_item2_no_change_timestamp = copy.deepcopy(d1_item2)

        d1_item3 = d1_item3_no_change_timestamp = \
            consistent_sdb.connection.get_attributes(
                                               settings.test_domains[0],
                                               self.test_items[2]
                                              )
        d1_item3_no_change_timestamp = copy.deepcopy(d1_item3)

        d2_item1 = d2_item1_no_change_timestamp = \
            consistent_sdb.connection.get_attributes(
                                               settings.test_domains[1],
                                               self.test_items[0]
                                              )
        d2_item1_no_change_timestamp = copy.deepcopy(d2_item1)

        # delete the last change timestamp attribute
        del d1_item1_no_change_timestamp[consistent_sdb.last_changed_attribute_key()]
        del d1_item2_no_change_timestamp[consistent_sdb.last_changed_attribute_key()]
        del d1_item3_no_change_timestamp[consistent_sdb.last_changed_attribute_key()]
        del d2_item1_no_change_timestamp[consistent_sdb.last_changed_attribute_key()]

        # Check changes applied correctly
        # domain a
        self.assertEqual(
                         d1_item1_no_change_timestamp,
                         {'a': ['1', '2']}
                        )

        self.assertEqual(
                         d1_item2_no_change_timestamp,
                         {'c': ['6', '7', '8']}
                        )

        self.assertEqual(
                         d1_item3_no_change_timestamp,
                         {}
                        )

        self.assertEqual(
                         d2_item1_no_change_timestamp,
                         {'f': ['12']}
                        )

        # Check action timestamp entered to the item's journal of each changed
        # item
        self.assertEqual(
                         consistent_sdb.journals_db.lrange(
                                                   consistent_sdb.actions_journal_key(
                                                            settings.test_domains[0],
                                                            self.test_items[0]
                                                                      ),
                                                   -1,
                                                   -1
                                                  ),
                         d1_item1[consistent_sdb.last_changed_attribute_key()]
                        )

        self.assertEqual(
                         consistent_sdb.journals_db.lrange(
                                                   consistent_sdb.actions_journal_key(
                                                            settings.test_domains[0],
                                                            self.test_items[1]
                                                                      ),
                                                   -1,
                                                   -1
                                                  ),
                         d1_item2[consistent_sdb.last_changed_attribute_key()]
                        )

        self.assertEqual(
                         consistent_sdb.journals_db.lrange(
                                                   consistent_sdb.actions_journal_key(
                                                            settings.test_domains[0],
                                                            self.test_items[2]
                                                                      ),
                                                   -1,
                                                   -1
                                                  ),
                         d1_item3[consistent_sdb.last_changed_attribute_key()]
                        )

        self.assertEqual(
                         consistent_sdb.journals_db.lrange(
                                                   consistent_sdb.actions_journal_key(
                                                            settings.test_domains[1],
                                                            self.test_items[0]
                                                                      ),
                                                   -1,
                                                   -1
                                                  ),
                         d2_item1[consistent_sdb.last_changed_attribute_key()]
                        )
         
        # Check action item created in redis for each item action performed 
        self.assertEqual(
                         pickle.loads(str(consistent_sdb.logs_db.get(
                                                         consistent_sdb.action_log_key(
                                                                  settings.test_domains[0],
                                                                  self.test_items[0],
                                                                  d1_item1[consistent_sdb.last_changed_attribute_key()][0]
                                                                                )
                                                        ))),
                         {'action': 'delete', 'attributes': delete_records[settings.test_domains[0]][self.test_items[0]]}
                        )

        self.assertEqual(
                         pickle.loads(str(consistent_sdb.logs_db.get(
                                                         consistent_sdb.action_log_key(
                                                                  settings.test_domains[0],
                                                                  self.test_items[1],
                                                                  d1_item2[consistent_sdb.last_changed_attribute_key()][0]
                                                         )
                                                        ))),
                         {'action': 'delete', 'attributes': delete_records[settings.test_domains[0]][self.test_items[1]]}
                        )

        self.assertEqual(
                         pickle.loads(str(consistent_sdb.logs_db.get(
                                                         consistent_sdb.action_log_key(
                                                                  settings.test_domains[0],
                                                                  self.test_items[2],
                                                                  d1_item3[consistent_sdb.last_changed_attribute_key()][0]
                                                                                )
                                                        ))),
                         {'action': 'delete', 'attributes': delete_records[settings.test_domains[0]][self.test_items[2]]}
                        )

        self.assertEqual(
                         pickle.loads(str(consistent_sdb.logs_db.get(
                                                         consistent_sdb.action_log_key(
                                                                  settings.test_domains[1],
                                                                  self.test_items[0],
                                                                  d2_item1[consistent_sdb.last_changed_attribute_key()][0]
                                                                                )
                                                        ))),
                         {'action': 'delete', 'attributes': delete_records[settings.test_domains[1]][self.test_items[0]]}
                        )

        # Check ttl was set for the cached action items
        self.assertTrue(
             consistent_sdb.logs_db.ttl(
                  consistent_sdb.action_log_key(
                           settings.test_domains[0],
                           self.test_items[0],
                           d1_item1[consistent_sdb.last_changed_attribute_key()][0]
                  )
             ) > 0
        )
        
        self.assertTrue(
             consistent_sdb.logs_db.ttl(
                  consistent_sdb.action_log_key(
                           settings.test_domains[0],
                           self.test_items[1],
                           d1_item2[consistent_sdb.last_changed_attribute_key()][0]
                  )
             ) > 0
        )

        self.assertTrue(
             consistent_sdb.logs_db.ttl(
                  consistent_sdb.action_log_key(
                           settings.test_domains[0],
                           self.test_items[2],
                           d1_item3[consistent_sdb.last_changed_attribute_key()][0]
                  )
             ) > 0
        )

        self.assertTrue(
             consistent_sdb.logs_db.ttl(
                  consistent_sdb.action_log_key(
                           settings.test_domains[1],
                           self.test_items[0],
                           d2_item1[consistent_sdb.last_changed_attribute_key()][0]
                  )
             ) > 0
        )

    def test_02_put(self):
        # initiate the test items
        # domain a
        consistent_sdb.connection.delete_attributes(settings.test_domains[0], self.test_items[0])
        consistent_sdb.connection.delete_attributes(settings.test_domains[0], self.test_items[1])
        consistent_sdb.connection.delete_attributes(settings.test_domains[0], self.test_items[2])
        # domain b
        consistent_sdb.connection.delete_attributes(settings.test_domains[1], self.test_items[0])

        # put records to more than one domain at once.
        put_records = {
                       settings.test_domains[0]: {
                        self.test_items[0]: {
                         'attribute_a': {
                          'values': set(['a', 'b']),
                          'replace': False
                         }
                        },
                        self.test_items[1]: {
                         'attribute_b': {
                          'values': set(['c', 'd']),
                          'replace': False
                         }
                        }
                       },
                       settings.test_domains[1]: {
                        self.test_items[0]: {
                         'attribute_a': {
                          'values': set(['a', 'b']),
                          'replace': False
                         }
                        }
                       }
                      }

        consistent_sdb.put(put_records)

        records_state = {} # the keys are in the form of domain_names::item the
                           # values are the attribute received from
                           # get_attributes for that item
        
        helpers.eventualConsistencySleep()
 
        for domain, items in put_records.items():
            for item in items:
                # Get the items we changed
                records_state[domain + '::' + item] = \
                    consistent_sdb.connection.get_attributes(domain, item)

                # check that the values that were put (except of the last
                # changed attribute) are the values we wanted.
                self.assertEqual(
                     # The items received
                     dict(
                          [
                           (attribute_name, attribute_properties) for
                           (attribute_name, attribute_properties) in
                           records_state[domain + '::' + item].items() if
                           attribute_name != consistent_sdb.last_changed_attribute_key()
                          ]
                         ),
                     # The items that were put
                     # keys are the item's attributes names the values are
                     # their values
                     dict(
                          [
                           (attribute_name, attribute_properties['values']) for
                           (attribute_name, attribute_properties) in
                           put_records[domain][item].items()
                          ]
                         )
                )

                # Check that the changes done for each item, were logged
                # correctly and expiry time was set for them

                # 1. Make sure the timestamp that was logged in the item's
                # journal by verifing its last record has the same value as the
                # item's last changed attribute value in the database
                self.assertEqual(
                    consistent_sdb.journals_db.lrange(
                        consistent_sdb.actions_journal_key(domain, item),
                        -1,
                        -1
                    )[0],
                    records_state[domain + '::' + item]
                                 [consistent_sdb.last_changed_attribute_key()]
                                 [0]
                )

                timestamp = records_state[domain + '::' + item]\
                                         [consistent_sdb.last_changed_attribute_key()]\
                                         [0]

                # 2. Make sure the correct values were put to the action log
                self.assertEqual(
                    pickle.loads(
                        str(consistent_sdb.logs_db.get(
                            consistent_sdb.action_log_key(domain, item, timestamp)
                        ))
                    ),
                    {'action': 'put', 'attributes': put_records[domain][item]}
                )

                # 3. Verify expiry time was set
                self.assertTrue(
                    consistent_sdb.logs_db.ttl(
                        consistent_sdb.action_log_key(domain, item, timestamp)
                    ) > 0
                )

    def test_03_get(self):
        # initiate the test items
        # domain a
        consistent_sdb.connection.delete_attributes(settings.test_domains[0], self.test_items[0])
        consistent_sdb.connection.delete_attributes(settings.test_domains[0], self.test_items[1])
        consistent_sdb.connection.delete_attributes(settings.test_domains[0], self.test_items[2])
        # domain b
        consistent_sdb.connection.delete_attributes(settings.test_domains[1], self.test_items[0])

        # put records to more than one domain at once.
        put_records = {
                       settings.test_domains[0]: {
                        self.test_items[0]: {
                         'attribute_a': {
                          'values': set(['a', 'b']),
                          'replace': False
                         }
                        },
                        self.test_items[1]: {
                         'attribute_b': {
                          'values': set(['c', 'd']),
                          'replace': False
                         }
                        }
                       },
                       settings.test_domains[1]: {
                        self.test_items[0]: {
                         'attribute_a': {
                          'values': set(['a', 'b']),
                          'replace': False
                         }
                        }
                       }
                      }

        consistent_sdb.put(put_records)

        helpers.eventualConsistencySleep()

        get_records = {
                       settings.test_domains[0]: {
                        self.test_items[0]: ['attribute_a'],
                        self.test_items[1]: []
                       },
                       settings.test_domains[1]: {
                        self.test_items[0]: []
                       },
                      }

        print consistent_sdb.get(get_records)
   
    def test_04_apply_latest_changes(self):
        journal_ttl = datetime.timedelta(seconds=consistent_sdb.settings.journal_ttl)
        now = datetime.datetime.utcnow()
 
        testing_domain = 'hypothetical_domain'
        testing_item = 'general_item'
        testing_item_actions_journal = \
            consistent_sdb.actions_journal_key(testing_domain, testing_item)
        # The test will use each of testing_item_dictionaries items with its
        # corresponding testing_item_timestamps as inputs for
        # consistent_sdb.apply_latest_changes()
        testing_item_dictionaries = [{'a': ['a', 'b']}, {'f': ['a']}, {'a': ['a'], 'e': ['b']}]

        testing_item_timestamps_relative_to_ttl = [1, 3, 5]
        testing_item_timestamps = [
            (now - journal_ttl + datetime.timedelta(seconds=relative_timestamp)).isoformat() for
            relative_timestamp in
            testing_item_timestamps_relative_to_ttl
        ]

        expected_results = [
                            {'a': set(['d', 'f']), 'e': set(['a']), 'f': set(['b'])},
                            {'e': set(['a'])},
                            {'a': set(['a']), 'e': set(['a','b'])}
                           ]
    
 
        # Initiate test item's journal
        consistent_sdb.journals_db.delete(testing_item_actions_journal)
    
        # Illustrate situation where we've done some actions on testing_item
        # Set the actions timestamps
        actions_timestamps_relative_to_ttl = [-1, 2, 4, 6]
        actions_timestamps = [
            (now - journal_ttl + datetime.timedelta(seconds=relative_timestamp)).isoformat() for
            relative_timestamp in
            actions_timestamps_relative_to_ttl
        ]
 
        # list of action logs, the timestamp that will be set for each action
        # is the corresponding item in actions_timestamps 
        actions = [
                   {
                       'action': 'put',
                       'attributes': {
                                      'c': {
                                            'values': ['a', 'b'],
                                            'replace': False
                                           },
                                      'd': {
                                            'values': ['a', 'b'],
                                            'replace': False
                                           }
                                     }
                   },
                   {
                       'action': 'put',
                       'attributes': {
                                      'a': {
                                            'values': ['f', 'd'],
                                            'replace': True
                                           },
                                      'e': {
                                            'values': ['a', 'b'],
                                            'replace': False
                                           },
                                      'f': {
                                            'values': ['a', 'b'],
                                            'replace': False
                                           }
                                     }
                   },
                   {
                       'action': 'delete',
                       'attributes': {
                                      'e': [],
                                      'f': ['a']
                                     }
                   },
                   {
                       'action': 'put',
                       'attributes': {
                                      'e': {
                                            'values': ['a'],
                                            'replace': False
                                           }
                                     }
                   }
                  ]
 
        for i, action_timestamp in enumerate(actions_timestamps):
            consistent_sdb.log_action(
                                testing_domain,
                                testing_item,
                                action_timestamp,
                                actions[i]['action'],
                                actions[i]['attributes']
                               )
 
        for i, item_dictionary in enumerate(testing_item_dictionaries):
            item_timestamp = testing_item_timestamps[i]
            self.assertEqual(
                consistent_sdb.apply_latest_actions(
                                              testing_domain,
                                              testing_item,
                                              item_dictionary,
                                              item_timestamp
                                             ),
                expected_results[i]
            )
 
 
    def test_05_apply_latest_changes_journaling_cleaning(self):
        journal_testing_domain = 'hypothetical_domain'
        journal_testing_item = 'general_item'
 
        # apply_latest_changes() should remove expired items from actions
        # journals - in the following lines, we'll test that.
        item_actions_journal = \
            consistent_sdb.actions_journal_key(journal_testing_domain, journal_testing_item)
 
        # Initiate test item's journal
        consistent_sdb.journals_db.delete(item_actions_journal)
 
        # Write few timestamps to the testing item's journal - some expired and
        # some that aren't
        journal_ttl = datetime.timedelta(seconds=consistent_sdb.settings.journal_ttl)
        now = datetime.datetime.utcnow()
 
        timestamps_to_enter_relative_to_ttl = [-10, -5, -1, 0, 1, 2, 3]
 
        timestamps_to_enter = [
            (now - journal_ttl + datetime.timedelta(seconds=relative_timestamp)).isoformat() for
            relative_timestamp in
            timestamps_to_enter_relative_to_ttl
        ]
        # set the action that will be associated with the journal records
        pseudo_action = 'delete'
        pseudo_action_attributes = {'a': 'a'}
 
        # Create the designed journal
        for timestamp in timestamps_to_enter:
            consistent_sdb.log_action(
                                journal_testing_domain,
                                journal_testing_item,
                                timestamp,
                                pseudo_action,
                                pseudo_action_attributes
                               )
 
        consistent_sdb.apply_latest_actions(
                                      journal_testing_domain,
                                      journal_testing_item,
                                      {},
                                      now.isoformat()
                                     )
 
        # check that after calling consistent_sdb.apply_latest_actions the amount of
        # records in the journal is equal to the amount of non-expired records
        # we've added to the journal
        self.assertEqual(
                        len(consistent_sdb.journals_db.lrange(item_actions_journal, 0, -1)),
                        len([i for i in timestamps_to_enter_relative_to_ttl if i > 0])
                       )
 
    def test_06_dict_delete(self):
        dictionary = {'a': ['a', 'b'], 'b': ['a']}
 
        delete_records = {'a': ['a'], 'c': ['d']}
        self.assertEqual(
                         {'a': OrderedSet(['b']), 'b': OrderedSet(['a'])},
                         consistent_sdb.dict_delete(dictionary, delete_records),
                        ) 
 
    def test_07_dict_put(self):
        dictionary = {'a': ['a', 'b'], 'b': ['a']}
 
        put_records = {
                       # Test replace
                       'a': {
                             'values': ['c'],
                             'replace': True
                            },
                       # Test append 
                       'b': {
                             'values': ['b'],
                             'replace': False 
                            },
                       # Test new attribute
                       'c': {
                             'values': ['a'],
                             'replace': False 
                            }
                      }
 
        self.assertEqual(
                         consistent_sdb.dict_put(dictionary, put_records),
                         {'a': set(['c']), 'b': set(['a','b']), 'c': set(['a'])}
                        )
 
    def test_08_select(self):
        put_records = {
                       settings.test_domains[0]: {
                        self.test_items[0]: {
                         'attribute_a': {
                          'values': set(['a', 'b']),
                          'replace': False
                         }
                        },
                        self.test_items[1]: {
                         'attribute_b': {
                          'values': set(['c', 'd']),
                          'replace': False
                         }
                        }
                       },
                       settings.test_domains[1]: {
                        self.test_items[0]: {
                         'attribute_a': {
                          'values': set(['a', 'b']),
                          'replace': False
                         }
                        }
                       }
                      }
 
        consistent_sdb.put(put_records)
    
        print consistent_sdb.select('attribute_b', settings.test_domains[0], 'attribute_b = "d"')
        # print consistent_sdb.get({settings.test_domains[0], self.test_items[0]})

if __name__ == '__main__':
    unittest.main()
