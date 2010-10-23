#!/usr/bin/python
# vim: set fileencoding=utf-8 :

"""The status module is used to centralize run-time status and statistics

"""

# counts https timeouts occurred
https_timeouts = 0

actions_count = {
                 'get_item': 0,
                 'delete_item': 0,
                 'put_attributes': 0,
                 'delete_attributes': 0,
                 'select': 0
                }

# List of dictionaries representing each of the key value db requests.
# Each dictionary supplies information about the request, its response and
# processing times
requests = []

def total_db_box_usage():
    global requests

    total = float()
    for request in requests:
        total += float(request['db_box_usage'])

    return '%.10f' % total
