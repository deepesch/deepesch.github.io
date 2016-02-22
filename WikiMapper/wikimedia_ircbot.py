# Andrew Mo (mo@andrewmo.com - moandcompany)
# https://github.com/moandcompany

"""
Wikimedia log bot - logs irc.wikimedia.org channel's events to a file.

Originally based off of Twisted Matrix Laboratory's IRC log bot example for Python

Modified for use in a data pipeline demonstration project for Insight Data Engineering 2015

- Future: Consider how to deal with log rotation in the future
- Future: Consider binding channel info to irc servers so the bot can connect to multiple irc servers simultaneously

"""

# twisted imports
from twisted.words.protocols import irc
from twisted.internet import reactor, protocol
from twisted.python import log

# system imports
import time, sys, os

# ircLogBot imports
from ircLogBot import LogBot, LogBotFactory

if __name__ == '__main__':
    # initialize logging
    log.startLogging(sys.stdout)

    # IRC Server configuration
    # Future:  These settings should be taken from a configuration file or external source
    server = "irc.wikimedia.org"
    port = 6667

    # IRC channel list file
    channel_list_file = "./channel_list.txt"

    # IRC channel log destination (e.g. '/var/log/wiki_ircbot/')
    channel_log_path = "./logs"

    # Kafka Broker for publishing (optional)
    broker_list = ["172.31.6.79:9092", "172.31.5.24:9092", "172.31.5.25:9092", "172.31.5.26:9092"]
    omnichannel = "firehose"


    # Check to ensure channel log path exists
    if not os.path.exists(channel_log_path):
        print "Channel Log file path does not exist. Creating path."
        os.makedirs(channel_log_path)

    # Create a list of channels for the IRC Channel sensors to target/listen to
    # Future: This list should be taken from external sources (e.g. bot control channel, external file, feedback loop)
    if not os.path.isfile(channel_list_file):
        print "Channel List file not found. Using default list."
        channel_list = ["#en.wikipedia"]
    else:
        with open(channel_list_file, 'r') as file:
            channel_list = file.read().splitlines()

        # Check if channel list is empty
        if not channel_list:
            print "Channel List file is empty. Shutting down."
            sys.exit("Error: Channel List file is empty.")

    print "Started ircLogBot on %s" % (time.asctime(time.gmtime()))
    print "Reading sensor target list:"

    for channel in channel_list:
        print "Found: " + channel

    print "Found %s targets for listening." % (len(channel_list))

    # create factory protocol and application
    print "Creating factory protocol and application list..."
    factory_list = {}

    # Create a string formatted date/timestamp using UTC time
    utc_timestamp = time.gmtime()
    timestamp = time.strftime('%Y-%m-%d_%H-%M-%S', utc_timestamp) + "-UTC"

    for channel in channel_list:
        log_name = channel_log_path + "/" + channel + "-" + timestamp + ".log"
        factory_list[channel] = LogBotFactory(channel, log_name, broker_list, omnichannel)

    # connect factory to this host and port
    for channel in factory_list:
        reactor.connectTCP(server, port, factory_list[channel])

    # Start running the ircLogBot
    reactor.run()
