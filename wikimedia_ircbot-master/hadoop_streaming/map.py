#!/usr/bin/env python

"""
A basic batch wikimedia irc message parser for use with Hadoop Streaming or shell pipes
"""

import os, sys
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.dirname(CURRENT_DIR))

import parse_irclog

# from collections import namedtuple


# Map input comes from STDIN (standard input)
for line in sys.stdin:

    # Remove leading and trailing whitespace characters
    line = line.strip()

    # use field parser (returns tuples)
    try:
        line = parse_irclog.parse_ircmsg(line)
        pass
    except Exception as e:
        pass


#   parse_ircmsg() returns a namedtuple with the following fields:
#   tuple_fields = ['timestamp', 'topic', 'url', 'user', 'size', 'flags', 'comment']

    if line:
        print '%s\t%s' % (line.user, 1)

    else:
        pass
