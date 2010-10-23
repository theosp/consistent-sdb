#!/usr/bin/python
# vim: set fileencoding=utf-8 :

import settings
from time import sleep
from sys import stdout
def eventualConsistencySleep(time=settings.min_sleep_time_to_avoid_inconsistency):
    stdout.write("Eventual Consistency sleep (%dsecs): " % time)
    while time:
        stdout.write(str(time) + ' ')
        stdout.flush()
        time -= 1
        sleep(1)

    print

def msg(object, message):
    print object.id() + ' :: ' + message
