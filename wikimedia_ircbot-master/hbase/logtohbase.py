#!/usr/bin/env python

"""
A basic bulk loader of parsed IRC log data to HBase using 'happybase'
"""

import os, sys
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.dirname(CURRENT_DIR))

import parse_irclog
import happybase

from datetime import datetime

connection = happybase.Connection('0.0.0.0')
topic_table = connection.table('topicactivity')
user_table = connection.table('useractivity')
event_daily_table = connection.table('dailyevents')
event_hourly_table = connection.table('hourlyevents')



topic_batch = topic_table.batch()
user_batch = user_table.batch()
event_daily_batch = event_daily_table.batch()
event_hourly_batch = event_hourly_table.batch()

batch_counter = 0


def iso_timestamp_to_hour(iso_timestamp):
    hour = datetime.strptime(iso_timestamp.replace('Z', 'GMT'),'%Y-%m-%dT%H:%M:%S%Z')
    hour = hour.replace(minute=0, second=0).isoformat()
    return hour

def iso_timestamp_to_day(iso_timestamp):
    day = datetime.strptime(iso_timestamp.replace('Z', 'GMT'),'%Y-%m-%dT%H:%M:%S%Z')
    day = day.replace(hour=0, minute=0, second=0).isoformat()
    return day


with open("test.log") as logfile:
    lines = logfile.readlines()



# Map input comes from an unparsed IRC logBot logfile
for line in lines:

    # Remove leading and trailing whitespace characters
    line = line.strip()

    # use field parser (returns tuples)
    try:
        line = parse_irclog.parse_ircmsg(line)
        pass
    except Exception as e:
        line = None

#   parse_ircmsg() returns a namedtuple with the following fields:
#   tuple_fields = ['timestamp', 'topic', 'url', 'user', 'size', 'flags', 'comment']

# We account for multiple events on the same topic, or by a user, at the same time by using column names that are a composite of timestamp and topic/edit URL; this has a storage penalty due to long column name length.
# TODO reevalute column naming scheme

    if line:
        day = iso_timestamp_to_day(line.timestamp)
        hour = iso_timestamp_to_hour(line.timestamp)
        
        topic_batch.put(line.topic, {'cf1:'+line.timestamp+'_'+line.url: str(line)})
        user_batch.put(line.user, {'cf1:'+line.timestamp+'_'+line.url: str(line)})

        event_daily_batch.put(day, {'cf1:'+line.timestamp+'_'+line.url: str(line)})
        event_hourly_batch.put(hour, {'cf1:'+line.timestamp+'_'+line.url: str(line)})

        batch_counter += 1

        # Divide HBase writes into batches for a ~10x+ load performance speedup
        if batch_counter % 500 == 0:

            topic_batch.send()
            user_batch.send()

            event_daily_batch.send()
            event_hourly_batch.send()

# Write all remaining events
event_daily_batch.send()
event_hourly_batch.send()
topic_batch.send()
user_batch.send()


print batch_counter
