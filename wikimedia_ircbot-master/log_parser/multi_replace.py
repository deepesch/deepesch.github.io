#!/bin/env python

import re

test_timestamp = "[2015-01-22T08:23:18Z]"
test_message = "[[2014 Wimbledon Championships]]  http://en.wikipedia.org/w/index.php?diff=643640156&oldid=643640087 * Gohqianyan832 * (+0) /* Day 9 (2 July) */"


"""
TODO
- Need to add a filter for control/status messages

Examples:
[2015-01-22T08:30:01Z] 	 [Connected at Thu Jan 22 08:30:01 2015 UTC]
[2015-01-22T08:23:01Z] 	 [I have joined #en.wikipedia]
[2015-01-22T08:41:42Z] 	 [Disconnected at Thu Jan 22 08:41:42 2015 UTC]

"""


# pythonic multiple replace using lambda functions
# based on http://code.activestate.com/recipes/81330-single-pass-multiple-replace/

def multiple_replace(dict, text):
  # Create a regular expression  from the dictionary keys
  regex = re.compile("(%s)" % "|".join(map(re.escape, dict.keys())))

  # For each match, look-up corresponding value in dictionary
  return regex.sub(lambda mo: dict[mo.string[mo.start():mo.end()]], text) 

from UserDict import UserDict 
class Xlator(UserDict):

  """ An all-in-one multiple string substitution class """ 

  def _make_regex(self): 

    """ Build a regular expression object based on the keys of
    the current dictionary """

    return re.compile("(%s)" % "|".join(map(re.escape, self.keys()))) 

  def __call__(self, mo): 
    
    """ This handler will be invoked for each regex match """

    # Count substitutions
    self.count += 1 # Look-up string

    return self[mo.string[mo.start():mo.end()]]

  def xlat(self, text): 

    """ Translate text, returns the modified text. """ 

    # Reset substitution counter
    self.count = 0 

    # Process text
    return self._make_regex().sub(self, text)


if __name__ == "__main__": 

  # define some parsing schemes

  timestamp_filter = {
    "[" : "",
    "]" : "",
    "T" : " ",
  }

  score_filter = {
    "(" : "",
    ")" : "",
  }

  summary_filter = {
    "/*" : "",
    "*/" : "",
  }

  message_filter = {
    "[[" : "",
    "]]" : "",
    "  " : "\t",
    " * ": "\t",
    "/*" : "\t",
    "*/" : "",
  }

  # Test Timestamp Filter
  print "Testing Timestamp Filter:"
  print "Before:\t" + test_timestamp
  print "After:\t" + multiple_replace(timestamp_filter, test_timestamp)

  print "\n"

  # Test Timestamp filter using Xlator class
  timestamp_xlator = Xlator(timestamp_filter)
  print "Before:\t" + test_timestamp
  print "After:\t" + timestamp_xlator.xlat(test_timestamp)
  print "%d character(s) changed." % timestamp_xlator.count

  print "\n"

  print "Testing Message Filter:"
  print "Before:\t" + test_message
  print "After:\t" + multiple_replace(message_filter, test_message)
