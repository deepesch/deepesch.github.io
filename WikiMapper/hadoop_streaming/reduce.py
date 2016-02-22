#!/usr/bin/env python

"""
Generic reducer example for Hadoop Streaming - uses word count model for field counts
"""

from operator import itemgetter
import sys

current_field = None
current_count = 0

field  = None

# Reduce input comes from STDIN
for line in sys.stdin:

    # remove leading and trailing whitespace characters
    line = line.strip()

    # parse input from mapper
    field, count = line.split('\t', 1)

    # convert count (string) to int
    try:
        count = int(count)

    except ValueError:
        # ignore value if not a value
        field = None
        continue

    if current_field == field:
        current_count += count

    else:
        if current_field:
            # Send result to STDOUT
            print '%s\t%s' % (current_field, current_count)
        current_count = count
        current_field = field


# do not forget to output the last word if needed!
if current_field == field:
    print '%s\t%s' % (current_field, current_count)
