#!/usr/bin/python
# vim: set fileencoding=utf-8 :

"""This module holds the settings used by all the aws_simpledb tests.

"""

from parent_dir_path import parent_dir_path

# We insert this path to sys.path's beginning (see startup module)
testing_subject_path = parent_dir_path(__file__, '-1')

test_domains = ['test_domain_1', 'test_domain_2']

# time in seconds to wait before checking whether a change applied to aws
# simpledb
min_sleep_time_to_avoid_inconsistency = 10 
