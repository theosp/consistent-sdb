#!/usr/bin/python
# vim: set fileencoding=utf-8 :

# amazon
amazon_db = 'sdb.amazonaws.com'
amazon_access_key_id = 'AKIAJEDCU3GDRME3I7CQ'
amazon_secret_access_key = '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'

# seconds to keep connection to amazon open until timeout
# 7 seconds works the best
amazon_timeout = 60

# each item in this list sets the time we will wait in seconds before its
# corresponding retry attempt, the amount of items determines the amount of
# retries (can be floats)
amazon_timeout_retries_delay = [0, 1, 2] # no delay before first retry
