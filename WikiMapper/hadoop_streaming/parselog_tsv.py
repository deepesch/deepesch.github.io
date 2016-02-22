#!/bin/env python

"""
Uses wikimedia irc message parser to convert raw wikimedia IRC log files into parsed messages, as tuples
"""

import os, sys
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.dirname(CURRENT_DIR))

import parse_irclog

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


    # parse_ircmsg() returns a namedtuple 
    if line:
        # print the named tuple with tab-separated values
        print "\t".join(tuple(line))
        pass
    else:
        pass
