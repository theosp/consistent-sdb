#!/usr/bin/python
# vim: set fileencoding=utf-8 :

"""Key value db layer for amazons simpledb

Implements session consistency layer.

"""

import datetime
import redis
import pickle

import settings
import status
from orderedset import OrderedSet

import aws_simpledb

from collections import defaultdict

connection = aws_simpledb.SimpleDB(
                                   settings.amazon_access_key_id,
                                   settings.amazon_secret_access_key,
                                   settings.amazon_db
                                  )

journals_db = redis.Redis(db=settings.actions_journals_redis_db)
logs_db = redis.Redis(db=settings.action_logs_redis_db)
# Note: The need for db seperation is documented on random_journal_cleaning()
# docstring

# DB ACTIONS:
def delete(records):
    """Deletes items, attributes or values from simpledb.

    (Domain deletion is not supported to avoid bad mistakes)

    Input:
    records structure:
    {
     'domain_A': {
                  # (1) to delete all attributes (entire item)
                  'item_a': {},

                  # (2) to delete all attribute's values
                  'item_b': {
                             'attribute_0': [],
                            },

                  # (3) to delete specific attribute's values
                  'item_c': {
                             # we will iterate over the values data-structure
                             # to find the values to delete. (so encapsulate
                             # even single value with a list or other compund
                             # data type)
                             'attribute_0': ['value_0', 'value_1']
                            }
                 },

     # record's logic would suggest that this should delete domain_B, but since
     # mistakes consequences might be catastrophic this option disabled.
     'domain_B': {}
    }

    """

    for domain, items in records.items():
        for item, attributes in items.items():
            if not attributes:
                connection.delete_attributes(domain, item)
            else:
                connection.delete_attributes(domain, item, attributes)

            # log action
            timestamp = current_timestamp()

            # Write timestamp to the server's last changed attribute in
            # simpledb
            # and log the action on redis
            connection.put_attributes(
                                      domain,
                                      item,
                                      {
                                       last_changed_attribute_key():
                                       {
                                        'values': [timestamp],
                                        'replace': True
                                       }
                                      }
                                     )

            log_action(domain, item, timestamp, 'delete', attributes)

def put(records):
    """Add's items, attributes and values to simpledb.

    Note: It is impossible to add an item without any attributes.
          It is impossible to add an attribute without values.

    Remember: Attributes are like set of values, i.e. Values should be
    unique and has no order.

    Input:
    records structure:
    {
     'domain_A': {
                  'item_a': {
                             # Values can also be list, just remember the order
                             # doesn't matter.
                             # Even single value should be inside a set or
                             # other compound data type.
                             'attribute_0': {
                                             'values': set(['value_0']),
                                             # If replace set to True the new
                                             # values will replace those that
                                             # attribute already had.
                                             # Otherwise they will be added to
                                             # them.
                                             'replace': True
                                            },
                             'attribute_1': {
                                             'values': set(['value_0', 'value_1']),
                                             'replace': False
                             }
                            },

                  'item_b': {
                             'attribute_0': {
                                             'values': set(['value_0']),
                                             # If replace set to True the new
                                             # values will replace those that
                                             # attribute already had.
                                             # Otherwise they will be added to
                                             # them.
                                             'replace': True
                                            },
                            }
                 },

     'domain_B': {
                  'item_a': {
                             'attribute_0': {
                                             'values': set(['value_0']),
                                             'replace': False 
                                            }
                            }
                 }
    }

    Implementation Note: If we change more than one item for domain, we use the
    sdb api batch_put_attributes command otherwise we use PutAttributes

    """

    timestamp = current_timestamp()

    # Add timestamp to the server's last changed attribute for each item we
    # change (used later for journaling)
    for domain, items in records.items():
        for item in items:
            records[domain][item][last_changed_attribute_key()] = \
                {
                 'values': [timestamp],
                 'replace': True
                }

    for domain, items in records.items():
        if len(items) > 1:
            connection.batch_put_attributes(domain, items)
        else:
            # if there is single item, put its attributes using
            # simpledb.put_attributes

            item, attributes = items.items()[0]
            connection.put_attributes(domain, item, attributes)

        # delete the last changed attribute for all the items
        for item, attributes in items.items():
            del records[domain][item][last_changed_attribute_key()]
            log_action(domain, item, timestamp, 'put', attributes)

def get(records):
    """Get items, or specific attributes from records

    This function uses apply_latest_actions() to keep server level consistency.

    The function doesn't return the last changed attribute, to keep the
    journaling layer transperent for the user.

    Input:
    records structure:
    {
     'domain_A': {
                  'item_a': [] # To get all the attributes
                 },
     'domain_B': {
                  'item_b': ['a', 'b'] # To get only the values for 'a' and 'b'
                 }
    }

    """

    result = {}

    for domain, items in records.items():
        result[domain] = {}
        for item, attributes in items.items():
            if attributes: # if attributes isn't empty add also the server's
                           # last changed attribute (otherwise we'll get it
                           # anyway)
                attributes.append(last_changed_attribute_key())

            result[domain][item] = \
                connection.get_attributes(domain, item, attributes)

            last_changed_attribute = \
                result[domain][item][last_changed_attribute_key()]

            # Apply changes done on that item after its timestamp.
            # Note: The following steps are taken only if changes to that item
            # had been done by this layer, we indicate that by checking whether
            # `last_changed_attribute` item has value
            if last_changed_attribute:
                timestamp = last_changed_attribute[0]

                del result[domain][item][last_changed_attribute_key()]

                result[domain][item] = \
                    apply_latest_actions(
                                         domain,
                                         item,
                                         result[domain][item],
                                         timestamp
                                        )
    
    return result

def select(output_list, domain_name, expression=None, sort_instructions=None, limit=None):
    # If output_list holds attribute name but isn't compound type, we enclose
    # it in a list
    if not hasattr(output_list, '__iter__') or isinstance(output_list, basestring):
        if not output_list in ['*', 'itemName()', 'count(*)']:
            output_list = [output_list]

    # If output_list is list it means it holds explicit list of attributes,
    # we add to the list last_changed_attribute_key() in order to be to apply
    # the journaling later
    if hasattr(output_list, '__iter__'):
        output_list.append(last_changed_attribute_key())

    results = connection.select(output_list, domain_name, expression, sort_instructions, limit)

    if not output_list in ['itemName()', 'count(*)']:
        # restructure the results to be more intuitive
        _results = {}

        for result in results:
            item_name, item_values = result.items()[0]
            _results[item_name] = item_values

        results = _results

        # if output_list isn't 'itemName()', 'count(*)' we apply the latest
        # actions to the results.
        for item_name, item_values in results.items():
            last_changed_attribute = item_values[last_changed_attribute_key()]

            # Apply changes done on that item after its timestamp.
            # Note: The following steps are taken only if changes to that item
            # had been done by this layer, we indicate that by checking whether
            # `last_changed_attribute` item has value
            if last_changed_attribute:
                timestamp = last_changed_attribute[0]

                del results[item_name][last_changed_attribute_key()]

                results[item_name] = \
                    apply_latest_actions(
                                         domain_name,
                                         item_name,
                                         results[item_name],
                                         timestamp
                                        )

    return results

# LOCAL ACTIONS:
# The following functions implements the delete and put actions on a
# dictionary of sets. i.e. simulate the result of sdb actions performed on
# dictionary.
#
# If the dictionary has values other than sets they will be transformed to
# sets, If we wont be able to do it is an error.
# 
# We use this functions to apply changes on items we receive from simpledb that
# hold content that isn't up to date with changes done from this server (due to
# the natural inconsistency of simpledb)
#
# In this functions we look on the dictionary on which we perform the actions
# as a simpledb item, in opposite to the domain perspective of put() and
# delete(), thus the dictionary has to be dictionary of sets, and the
# input attribute parallel to the 'records' attribute of put() and delete()
# shouldn't have the domain addressing level and the item level.
def dict_delete(dictionary, records):
    """Delete items from dictionary according to the instructions in records,
    which defined the same way as in delete() records attribute item level.

    records structure:
    records == {} <==> delete all dictionary items
    records == {'attribute': set([])} <==> delete specific attribute
    records == {'attribute': set(['a'])} <==> Delete 'a' from attribute's set

    Notes:
    1. This function doesn't change dictionary (or records) - it returns new
    dictionary it generates.
    2. The records dictionary values doesn't have to be sets, any compound data
    type that can be transformed to set is valid.

    """

    # If records is empty
    if not records: 
        return {}

    # Rebuild records to have only attributes from dictionary typecast all
    # attribute's values to sets
    strict_records = defaultdict(set)
    for attribute, values in records.items():
        if attribute in dictionary:
            strict_records[attribute] = OrderedSet(values)

    result = dict([
                    (attribute, OrderedSet(values) - strict_records[attribute]) for
                    (attribute, values) in
                    dictionary.items() if
                    # select only values didn't mentioned in records, and those
                    # that were mentioned, but with specific values (their
                    # values set isn't empty)
                    not attribute in records or strict_records[attribute]
                  ])

    # remove empty sets from result
    return dict([(attribute, values) for (attribute, values) in result.items() if values])

def dict_put(dictionary, records):
    """Put items to dictionary according to the instructions in records,
    which defined the same way as in put() records attribute item level.

    records structure:
    records = {
               'attribute_0': {
                               'values': set(['value_0']),
                               # If replace set to True the new values will
                               # replace those that attribute already has.
                               # Otherwise they will be added to them.
                               'replace': True
                              },
               'attribute_1': {
                               'values': set(['value_0']),
                               'replace': False
                              }
              }

    Notes:
    1. This function doesn't change dictionary (or records) - it returns new
    dictionary it generates.
    2. The records dictionary values doesn't have to be sets, any compound data
    type that can be transformed to set is valid.

    """

    strict_dictionary = defaultdict(set)
    # move dictionary's items to strict_dictionary, transform values to sets
    for attribute, values in dictionary.items():
        strict_dictionary[attribute] = OrderedSet(values)

    # typecast all attribute's values to sets
    for attribute, attribute_properties in records.items():
        values = OrderedSet(attribute_properties['values'])
        if attribute_properties['replace']:
            strict_dictionary[attribute] = values
        else:
            strict_dictionary[attribute] = \
                OrderedSet(strict_dictionary[attribute])
            strict_dictionary[attribute].union(values)

    return strict_dictionary

# JOURNALING RELATED FUNCTIONS:
# The journaling mechanisem log all the changes we do on simpledb items from
# this server.
#
# The journaling let us gain item consistency for all the actions performed
# from this server, i.e. to overcome simpledb inconsistency in the server
# level.
#
# The way journaling works:
# After each change done on simpledb a call to log_action has to be performed.
# log_action adds the change timestamp to a list on redis unique for that item
# and also creates record unique to the item and the timestamp with the action
# performed.
# TODO documentation needs work
def log_action(domain, item, timestamp, action, attributes=None):
    """This function should be called after each change on simpledb.

    It logs the change on redis.

    """

    action_log = pickle.dumps({
                  'action': action,
                  'attributes': attributes
                 })

    logs_db.set(action_log_key(domain, item, timestamp), action_log)

    # expire after it safe to assume that simpledb will be consistent for that
    # action
    logs_db.expire(
                   action_log_key(domain, item, timestamp),
                   settings.journal_ttl
                  )

    # Add the timestamp to the list represents the item's actions journal.
    #
    # we use the item's journal list to store only the timestamps, and not the
    # entire action logs, to avoid the need to pickle each action item to find
    # it's timestamp (reduce cpu) and to be able to apply the redis command
    # `expire` on the action log key, something we can't do on list items
    # (reduce memory usage)
    journals_db.push(actions_journal_key(domain, item), timestamp)

def apply_latest_actions(domain, item_name, item_dictionary, item_timestamp):
    """Go over the actions log and look for actions performed on item after
    timestamp, if such actions were found we perform them on item_dictionary
    in the same order they performed, and return it.

    otherwise we return item_dictionary as is.

    """

    item_datetime = parse_timestamp(item_timestamp)

    item_journal =\
        journals_db.lrange(actions_journal_key(domain, item_name), 0, -1)

    action_log_ttl = datetime.timedelta(seconds=settings.journal_ttl)

    # Go over the item's journal delete entries we find expired, apply on
    # item_dictionary those that made after its timestamp
    for timestamp in item_journal:
        log_entry_datetime = parse_timestamp(timestamp)

        # If the entry ttl passed delete it
        if datetime.datetime.utcnow() - log_entry_datetime >= action_log_ttl:
            journals_db.lrem(actions_journal_key(domain, item_name), timestamp, 1)
            continue

        if item_datetime < log_entry_datetime:
            action_log = pickle.loads(
                str(logs_db.get(action_log_key(domain, item_name, timestamp)))
            )

            if action_log['action'] == 'delete':
                item_dictionary =\
                    dict_delete(item_dictionary, action_log['attributes'])

            if action_log['action'] == 'put':
                item_dictionary =\
                    dict_put(item_dictionary, action_log['attributes'])

            status.latest_changes_applied += 1

    return item_dictionary

def action_log_key(domain, item, timestamp):
    """Returns the redis key for the action log for the action performed on
    item in timestamp
    
    """

    return domain + ':' + item + ':' + timestamp

def actions_journal_key(domain, item):
    """Returns the redis key that holds the timestamps list for the actions log
    available for item in redis. (the item's action journal)
    
    """

    return domain + ':' + item

def last_changed_attribute_key():
    """For each change we do on an item using this module we save the change
    timestamp.

    The change timestamp is specific to a server.
    
    This function returns the attribute name, we use to store the change
    timestamps for this server.

    """

    return 'last_changed::' + settings.server_id

def random_journal_cleaning():
    """Picks random action journal and deletes records from it.
    Records expiry time is determined by settings.journal_ttl. 

    It is impossible to set ttl for specific redis list item (redis 1.2), so
    inorder to keep the redis size as small as needed, for efficiancy and
    disksize saving we use this function from time to time to delete unneeded
    records.

    Note 1: apply_latest_actions() deletes expired records in the item's
    journal it was called for.
    Note 2: setting ttl for general redis item is possible, so we didn't had to
    write function that does the same for action logs.
    Note 3: to be able to use redis random command and know for sure we
    received actions journal we seperate the action journals db from the
    actions journals db

    """

    randomkey = journals_db.randomkey()

    # if empty key name returned (most probably means there are no keys) we
    # don't do anything
    if not randomkey:
        return

    item_journal = journals_db.lrange(randomkey, 0, -1)
    action_log_ttl = datetime.timedelta(seconds=settings.journal_ttl)

    # delete entries we find expired
    for timestamp in item_journal:
        log_entry_datetime = parse_timestamp(timestamp)

        # If the entry ttl passed delete it
        if datetime.datetime.utcnow() - log_entry_datetime >= action_log_ttl:
            journals_db.lrem(randomkey, timestamp, 1)
            status.random_expired_items_deletes += 1

# Helpers:
def current_timestamp():
    """Returns a string representing the current date and time in ISO 8601
    
    All the timestamps in this module are in the UTC tz.

    """

    return datetime.datetime.utcnow().isoformat()

def parse_timestamp(timestamp):
    """Get ISO 8601 string of the format:"%Y-%m-%dT%H:%M:%S.%f" and returns
    datetime.datetime object set according to this string
    
    """

    format = "%Y-%m-%dT%H:%M:%S.%f"

    return datetime.datetime.strptime(timestamp, format)

# We perform cleaning on some random actions journal on each load of this
# module.
# See random_journal_cleaning() docstring: 
for i in range(settings.random_journal_cleans):
    random_journal_cleaning()
