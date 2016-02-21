#!/bin/env python

import re
import dateutil.parser

import json

from collections import namedtuple


"""
Design considerations

- We want to be able to reuse our IRC channel message parser in a batch processing and stream processing engine

- Common parser methods in Python could be used in both paradigms (e.g. Hadoop Streaming and Spark Streaming)

- Tuples could/should be used to minimize footprint

- Parsing could be divided into a couple of workflow phases:

- Batch Processing:
- (1) Log Loading, one message per line
- (2) Message processing, by line
- (3) Message cleaning, by segment
--- (3.1) Timestamp
--- (3.2) Topic
--- (3.3) URL
--- (3.4) User/IP
--- (3.5) Change Size
- (4) Message reformatting, as a tuple

TODO:
- Stream Processing Concerns
-- A circular buffer data structure and state machine for tracking a message stream are likely needed
-- We do not want to have to re-process existing buffered messages on each cycle/iteration/processing interval

- Circular Buffer
--- It will need to be appended to / trimmed, so this should behave like a list
--- Buffer contents could/should be tuples
--- If multiple windows are applied to the same data, multiple buffers or one buffer with many windows?

"""

message_fields = ['timestamp', 'topic', 'url', 'user', 'size', 'flags', 'comment']

Parsed_Message = namedtuple("Parsed_Message", message_fields, verbose=False, rename=False)


def read_irclog(logfile):
    """
    Returns a list of IRC log messages split into tuples of two segments: (0) a timestamp, (1) a message
    """
    with open (logfile, 'r') as file:
        irclog = file.read().splitlines()

        # need to implement error handling if file does not exist
        # check if logfile is empty
        if not irclog:
            print "Warning: Log file is empty."
            sys.exit("Log file is empty.")
        else:
            return irclog


def parse_ircmsg(message):
    """
    Returns a tuple of wikimedia IRC channel message contents

    Returns an empty tuple for control messages (e.g. channel join/disconnect, user account actions)

    As channel message contents vary, this method discards all change summaries appearing after change size

    :param message:
    :return:
    """

    p = re.compile(r'\[(.*?)\]')
    m = p.search(message)
    timestamp = m.groups()[0]

    # regex scheme:   topic   flag(s)      url               user       size   comment
    # TODO: fix regex below -- currently leaves a leading whitespace in extracted comment
    p = re.compile(r'\[\[(.+?)\]\] (.*) (https?://[^\s]+) \* (.*?) \* \((.*?)\)(.*)')

    # split raw message into fields using above regex
    m = p.findall(message)

    if m:
        m = m[0]

        topic = m[0]
        flags = m[1]
        url = m[2]
        user = m[3]
        size = m[4]
        comment = m[5].strip()

        parsed_message = Parsed_Message(timestamp=timestamp, topic=topic, url=url, user=user, size=size, flags=flags, comment=comment)

        return parsed_message

    else:
        return ()


def parse_ircmsg_extended(message):
    """
    Returns parsed IRC message as a compact JSON formatted dictionary, with extended fields

    Returns an empty dict for control messages (e.g. channel join/disconnect, user account actions)

    TODO:  Add additional extended fields (e.g. direct url to topic), etc
    """

    m = parse_ircmsg(message)

    if m:

        timestamp = m.timestamp
        topic = m.topic
        flags = m.flags
        url = m.url
        user = m.user
        size = m.size
        comment = m.comment

        # package parsed irc message as dictionary
        temp = {}

        # original fields
        temp["timestamp"] = timestamp
        temp["topic"] = topic
        temp["url"]= url
        temp["user"] = user
        temp["size"] = int(size)
        temp["comment"] = comment
        temp["flags"] = flags

        # meta fields: flag related
        temp["minor"] = False
        temp["new_page"] = False
        temp["robot"] = False
        temp["unpatrolled"] = False


        # meta fields: editor type
        temp["anonymous"] = False

        # meta fields: topic context
        temp["topic_talk"] = True if 'Talk:' in topic else False
        temp["topic_user"] = True if 'User:' in topic else False
        temp["topic_wikipedia"] = True if 'Wikipedia:' in topic else False

        # a basic check to see if 'user' name is an IPv4 address (*.*.*.*)
        # note: invalid addresses such as 999.999.999.999 will match this regex
        # TODO:  improve IP match regex for IPv4 and IPv6 addresses
        p = re.compile(r'^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$')
        m = p.findall(user)

        temp["anonymous"] = True if m else False

        # process flags
        if flags:
            temp["minor"] = True if 'M' in flags else False
            temp["new_page"] = True if 'N' in flags else False
            temp["robot"] = True if 'B' in flags else False
            temp["unpatrolled"] = True if '!' in flags else False

    else:
        temp = {}

    # format output as compact JSON
    json_output = json.dumps(temp, separators=(',',':'))

    # format output as verbose JSON
    #json_output = json.dumps(temp, sort_keys=True, indent=4, separators=(',', ': '))
    return json_output


if __name__ == '__main__':

    # module test code
    testlog = "./test.log"
    test = read_irclog(testlog)

    print "Read in %s messages\n" % len(test)


    for line in test:
        # print line
        parsed_message = parse_ircmsg(line)

        # ignore empty tuples (channel control messages)
        if parsed_message:
            print parsed_message

            # print "Parsed message tuple length: %s\n" % (len(parsed_message))
        else:
            # print "Found control message -- Ignoring\n"
            pass


    # Use list compreshion
    print "\nUsing List Comprehension:"
    parsed_message_stream = [parse_ircmsg(line) for line in test]
    for item in parsed_message_stream:
        print item
    print "Parsed messages: %s" % len(parsed_message_stream)


    # Use 'filter()
    print "\nUsing List Filter:"
        # remove empty tuples from message list using filter
    filtered_message_stream = filter(None, parsed_message_stream)

    print "Filtered messages: %s\n" % len(filtered_message_stream)


    # # convert timestamp strings into datetime objects
    # stamps = [dateutil.parser.parse(event[0]) for event in filtered_message_stream]
    # print stamps
    # print [str(stamp) for stamp in stamps]
    #
    #
    # test extended irc message parser
    print "Read in %s messages\n" % len(test)

    for line in test:
        parsed_message = parse_ircmsg_extended(line)

        print parsed_message

        # ignore empty tuples (channel control messages)
        # if parsed_message:
        #     print "Parsed message tuple length: %s\n" % (len(parsed_message[0]))
        # else:
        #     print "Found control message -- Ignoring\n"
