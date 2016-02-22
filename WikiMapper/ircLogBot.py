__author__ = 'andrew'
# Andrew Mo (mo@andrewmo.com - moandcompany)
# https://github.com/moandcompany
#
# Based on:  https://twistedmatrix.com/documents/11.1.0/words/examples/ircLogBot.py
# Original code is Copyright (c) Twisted Matrix Laboratories.
# See LICENSE for details. (MIT License)
# https://twistedmatrix.com/trac/browser/trunk/LICENSE

"""
An IRC log bot - logs an IRC channel's events to a file.

Originally based off of Twisted Matrix Laboratory's IRC log bot example for Python

Modified for use in a data pipeline demonstration project for Insight Data Engineering 2015

- Timestamps have been modified to be ISO 8601 / RFC 3339 compliant
- Reference: https://www.ietf.org/rfc/rfc3339.txt

- Modified classes to enable optional publishing to a Kafka Broker
- Modified classes to enable operation in Kafka publishing only mode (skip file logging)

"""


# twisted imports
from twisted.words.protocols import irc
from twisted.internet import reactor, protocol
from twisted.python import log

# system imports
import time, sys, re

# kafka-python imports
import kafka
import parse_irclog

class MessageLogger:
    """
    An independent logger class (because separation of application
    and protocol logic is a good thing).
    """
    def __init__(self, file, channel, brokerlist="", omnichannel=""):
        self.file = file
        self.channel = channel

        # Kafka Publishing Modifications
        self.brokerlist = brokerlist
        self.omnichannel = omnichannel

        # check if kafka broker information was provided as constructor argument
        if self.brokerlist:
            try:
                self.mykafka = kafka.KafkaClient(hosts=self.brokerlist)
                self.producer = kafka.SimpleProducer(self.mykafka)
            except kafka.common.KafkaUnavailableError as e:
                print "Error: Kafka Broker specified but unreachable."
                print "Error: %s" % (e)

                # TODO: re-evaluate if LogBot should terminate upon broker failure
                # prefer to continue standard channel logging to file
                # TODO: need to address scenario for restarting kafka publishing

                # terminate the thread
                # sys.exit(1)
                # terminate the program
                #sys.quit()

                # continue LogBot operations and channel logging to file
                # MessageLogger.log() uses self.brokerlist to determine Kafka publishing
                # disable kafka publishing
                self.brokerlist=""

                print "Control: %s unreachable" % (self.brokerlist)
                print "Control: Disabling Kafka publishing..."

            if self.brokerlist:
                print "Publishing %s data to broker: %s" % (self.channel, self.brokerlist)
                if self.omnichannel:
                    print "Omnichannel specified. Also publishing to topic: %s" % (self.omnichannel)
            else:
                print "Kafka publishing was disabled due to broker availability."
        else:
            print "Kafka publishing was disabled due to no broker infomation."

    def log(self, message):
        """Write a message to the log file and/or publish to a Kafka broker"""

        # Generate a string formatted UTC timestamp with date
        utc_timestamp = time.gmtime()
        timestamp = time.strftime('[%Y-%m-%dT%H:%M:%SZ]', utc_timestamp)

        if self.file:
                self.file.write('%s \t %s\n' % (timestamp, message))
                self.file.flush()

        # Kafka Publishing Modifications
        # MessageLogger uses self.brokerlist to determine Kafka publishing

        # MessageLogger will attempt to publish to a Kafka topic if
        # (1) a Kafka broker was specified, and
        # (2) the Kafka broker was available
        if self.brokerlist:
            # prepare a message to be set to a kafka topic
            kafka_message = timestamp + "\t" + message

            # parse and clean channel message to tuple before sending
            # note that control messages are filtered out by the parser as empty tuples
            kafka_message = parse_irclog.parse_ircmsg_extended(kafka_message)

            # we will send channel sourced messages to a similarly named kafka topic
            # clean the channel name to remove invalid characters
            kafka_topicname = re.sub('[^A-Za-z0-9-_.]+', '', self.channel)

            # Future: clean this up and research kafka-python module issues
            # exception handling for other issues such as kafka broker not available etc...
            # exception handling section is incomplete
            # TODO: need to address scenario where kafka broker becomes unavailable
            try:
                if kafka_message and kafka_message != "{}":
                    # note: was getting type errors from kafka.producer.send_messages()
                    # cast parsed tuple as string before sending to kafka broker
                    self.producer.send_messages(kafka_topicname, str(kafka_message))

                    # if omnichannel mode is enabled, also broadcast to named omnichannel topic
                    if self.omnichannel:
                        self.producer.send_messages(self.omnichannel, str(kafka_message))

            except kafka.common.LeaderNotAvailableError as e:
                # kafka-python can raise exceptions if an auto-created topic is published to
                # https://github.com/mumrah/kafka-python/pull/109
                # https://github.com/mumrah/kafka-python/issues/249

                print "Error: Kafka producer - topic leader unavailable"
                print "Error: %s" % (e)

                # wait 1 sec and retry sending the message
                time.sleep(1)
                self.producer.send_messages(kafka_topicname, kafka_message)

                # if omnichannel mode is enabled, also broadcast to named omnichannel topic
                if self.omnichannel:
                    self.producer.send_messages(self.omnichannel, str(kafka_message))

            except Exception as e:
                print "Error: Kafka producer - other error"
                print "Error: Kafka broker down?"
                print "Error: %s" % (e)


    def close(self):
        if self.file:
            self.file.close()


class LogBot(irc.IRCClient):
    """A logging IRC bot."""

    nickname = "wikimedia_tracker"

    def connectionMade(self):
        irc.IRCClient.connectionMade(self)
    
        # default logger is started in append-mode
            # modified to allow pass-through of kafka broker information
        # modified to allow pass-through of empty file-name to indicate no file logging
        if self.factory.filename:
            self.logger = MessageLogger(open(self.factory.filename, "a"), self.factory.channel, self.factory.brokerlist, self.factory.omnichannel)
        else:
            self.logger = MessageLogger(self.factory.filename, self.factory.channel, self.factory.brokerlist, self.factory.omnichannel)
    
            self.logger.log("[Connected at %s UTC]" %
                            time.asctime(time.gmtime()))

    def connectionLost(self, reason):
        irc.IRCClient.connectionLost(self, reason)
        self.logger.log("[Disconnected at %s UTC]" %
                        time.asctime(time.gmtime()))
        self.logger.close()


    # callbacks for events

    def signedOn(self):
        """Called when bot has succesfully signed on to server."""
        self.join(self.factory.channel)

    def joined(self, channel):
        """This will get called when the bot joins the channel."""
        self.logger.log("[I have joined %s]" % channel)

    def privmsg(self, user, channel, msg):
        """This will get called when the bot receives a message."""
        user = user.split('!', 1)[0]

        # Strip IRC message control formatting (e.g. text color attributes)
        # See: http://twistedmatrix.com/documents/current/words/howto/ircclient.html
        clean_message = irc.stripFormatting(msg)
        self.logger.log("%s" % (clean_message))

        # Check to see if they're sending me a private message
        if channel == self.nickname:
            msg = "It isn't nice to whisper!  Play nice with the group."
            self.msg(user, msg)
            return

        # Otherwise check to see if it is a message directed at me
        if msg.startswith(self.nickname + ":"):
            msg = "%s: I am a log bot" % user
            self.msg(channel, msg)

    # For fun, override the method that determines how a nickname is changed on
    # collisions. The default method appends an underscore.
    def alterCollidedNick(self, nickname):
        """
        Generate an altered version of a nickname that caused a collision in an
        effort to create an unused related name for subsequent registration.
        """
        return nickname + '^'



class LogBotFactory(protocol.ClientFactory):
    """A factory for LogBots.

    A new protocol instance will be created each time we connect to the server.

    Modified constructor to allow option kafka_broker (FQDN:port) as argument
    """

    def __init__(self, channel, filename, brokerlist="", omnichannel=""):
        self.channel = channel
        self.filename = filename

        # store kafka_broker information for bot publishing (optional)
        self.brokerlist = brokerlist
        # store kafka 'omnichannel' topic name for bot publishing (optional)
        # if 'omnichannel' is named, all messages will also be logged to the named topic
        self.omnichannel = omnichannel

    def buildProtocol(self, addr):
        p = LogBot()
        p.factory = self
        return p

    def clientConnectionLost(self, connector, reason):
        """If we get disconnected, reconnect to server."""
        connector.connect()

    def clientConnectionFailed(self, connector, reason):
        print "connection failed:", reason
        reactor.stop()


if __name__ == '__main__':
    # initialize logging
    log.startLogging(sys.stdout)

    # create factory protocol and application
    # f = LogBotFactory(sys.argv[1], sys.argv[2])
    channel = "#en.wikipedia"
    logfile = channel + ".log"

    # test kafka publishing only mode
    logfile = ""

    brokerlist = ["0.0.0.0:9092"]
    omnichannel = "firehose"

    factory = LogBotFactory(channel, logfile, brokerlist, omnichannel)

    # connect factory to this host and port
    # reactor.connectTCP("irc.freenode.net", 6667, f)
    server = "irc.wikimedia.org"
    port = 6667
    reactor.connectTCP(server, port, factory)

    # run bot
    reactor.run()
