#!/usr/bin/python
# vim: set fileencoding=utf-8 :

server_id = 'dev_server'

# amazon
amazon_db = 'sdb.amazonaws.com' # None for aws_simpledb default
amazon_access_key_id = 'AKIAJEDCU3GDRME3I7CQ' # None for aws_simpledb default
amazon_secret_access_key = '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' # None for aws_simpledb default

# journal ttl, is the time in seconds we will keep journal items before
# deleting them. We believe that simpledb will become consistent for all the
# actions performed before this time, so it's safe to stop keeping them in the
# server's memory for session consistency.
journal_ttl = 60 * 5

# redis dbs
actions_journals_redis_db = '2'
action_logs_redis_db = '3'

# see consistent_sdb.random_journal_cleaning()
random_journal_cleans = 5

# prefix for cache keys
cache_prefix = 'lobserver:'
