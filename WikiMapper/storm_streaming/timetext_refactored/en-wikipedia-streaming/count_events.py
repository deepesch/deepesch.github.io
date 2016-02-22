from collections import defaultdict
from collections import namedtuple
import logging

from pyleus.storm import SimpleBolt

import MySQLdb
import time

import json

from operator import itemgetter

log = logging.getLogger('counter')

# TODO: Needs better documentation
# TODO: Incorporate Kafka feedback loop with aggregate stats
# TODO: Contuine refactoring and separation into more bolts/modules


class CountEventsBolt(SimpleBolt):

    # Passthrough for Database Information
    OPTIONS = ["db_host", "db_user", "db_pass", "db_name", "db_table", "db_table_1",\
        "db_table_2"]
    OUTPUT_FIELDS = ["count"]

    def initialize(self):

        self.events = 0

        # TODO: clean this up to reduce unnecessary redundancy
        self.topiclist = {}         # topics and their edit counts
        self.userlist = {}          # user names and their edit counts
        self.robotlist = {}         # bot names and net effect of their edits
        self.nonbotuserlist = {}    # non-bot user names and the net effect of their edits
        self.newpagelist = {}       # new pages and their urls
        self.editsizelist = {}      # topics and net size of related changes
        self.anonlist = {}          # anonymous user IPs and edit counts

        self.topicactivitylist = {} # topics and users who edited them
        self.useractivitylist = {}  # user names and urls for their edits

        # remember to check and ensure all state variables are emptied periodically
        self.robotedits = 0
        self.nonbotuseredits = 0
        self.editsize = 0
        self.newpages = 0


    def process_tuple(self, tup):
        line = tup.values[0]

        # bring tuple / line values into new variables for readability
        # consider using a named tuple
        topic = line["topic"]
        user = line["user"]
        editsize = line["size"]
        url = line["url"]

        self.events += 1
        self.log(line["topic"])
        #self.log(line["user"])


        # buffer topics
        # NOTE: we can filter out 'Talk:', 'User:', and 'Wikipedia:' topics here
        # consider moving the filter downstream
#        if not (line["topic_talk"] or line["topic_user"] or line["topic_wikipedia"]):
        if topic in self.topiclist:
            self.topiclist[topic] = (self.topiclist[topic] + 1)
        else:
            self.topiclist[topic] = 1

        # buffer users
        if user in self.userlist:
            self.userlist[user] = (self.userlist[user] + 1)
        else:
            self.userlist[user] = 1

        # buffer robot edits
        # the "robot" field is true if the edit was made by a robot
        if line["robot"]:
            self.robotedits += 1
            # we want to record the bot name from the "user" field in our tracker
            # we also track the net size of bot edit changes
            if user in self.robotlist:
                self.robotlist[user] = (self.robotlist[user] + editsize)
            else:
                self.robotlist[user] = editsize
        # the "robot" field is false if the edit was made by a user not-registered as a bot - could be anonymous
        else:
            # TODO: This counter is probably redunant as we already know/use: (edits - botedits == nonbotedits)
            self.nonbotuseredits += 1
            # we want to record the user name from the "user" field in our tracker
            # we also track the net size of nonbot user edit changes
            if user in self.nonbotuserlist:
                self.nonbotuserlist[user] = (self.nonbotuserlist[user] + editsize)
            else:
                self.nonbotuserlist[user] = editsize

        # buffer new page
        # the "new_page" field is true if the edit was a new page creation
        if line["new_page"]:
            self.newpages += 1
            # we want to record the topic name from the "topic" field and "url" in our tracker
            # we track the url to the most recent change
            self.newpagelist[topic] = url


        # buffer edit size
        self.editsize += editsize
        if topic in self.editsizelist:
            self.editsizelist[topic] = (self.editsizelist[topic] + editsize)
        else:
            self.editsizelist[topic] = editsize

        # buffer anon entries
        if line["anonymous"]:
            if user in self.anonlist:
                self.anonlist[user] = (self.userlist[user] + 1)
            else:
                self.anonlist[user] = 1

        # track all users editing a particular topic
        if topic in self.topicactivitylist:
            temp = self.topicactivitylist[topic] = self.topicactivitylist[topic]
            temp.append(user)
            self.topicactivitylist[topic] = temp
        else:
            self.topicactivitylist[topic] = [user, ]

        # track all topics edited by a particular user
        if user in self.useractivitylist:
            temp = self.useractivitylist[user]
            temp.append(url)
            self.useractivitylist[user] = temp
        else:
            self.useractivitylist[user] = [url, ]


    # Here's our tick tuple processor
    def process_tick(self):

        num_events = self.events

        # reset the event counter
        self.events = 0

        # generate ISO compliant UTC timestamp
        utc_timestamp = time.gmtime()
        timestamp = time.strftime('%Y-%m-%dT%H:%M:%SZ', utc_timestamp)
        
        #attributes
        #self.topiclist = {}
        #self.userlist = {}
        #self.robotlist = {}
        #self.newpagelist = {}
        #self.editsizelist = {}
        #self.anonlist = {}
        #self.nonbotlist = {}

        #self.topicactivitylist = {}
        #self.useractivitylist = {}

        ##########
        self.log("************************************************************")
        self.log("***** Events since last epoch: %d" % (num_events))
        #log.debug((timestamp, num_events))

        
        # 'topiclist'
        num_topics = len(self.topiclist)
        try:
            self.log("topics: %d" % (num_topics))
            json_topiclist = json.dumps(self.topiclist)
        except Exception as e:
            self.log(e)
        self.topiclist = {}

        # 'userlist'
        num_users = len(self.userlist)
        try:
            self.log("numusers: %d" % (num_users))
            json_userlist = json.dumps(self.userlist)
        except Exception as e:
            self.log(e)
        self.userlist = {}

        # 'robotedits'
        num_robots = len(self.robotlist)
        try:
            self.log("robots: %d" % (num_robots))
            json_robotlist = json.dumps(self.robotlist)
        except Exception as e:
            self.log(e)
        self.robotlist = {}

        # 'nonbotuseredits'
        num_nonbotusers = len(self.nonbotuserlist)
        try:
            self.log("nonbotusers: %d" % (num_nonbotusers))
            json_nonbotuserlist = json.dumps(self.nonbotuserlist)
        except Exception as e:
            self.log(e)
        self.nonbotuserlist = {}

        # 'newpages'
        num_newpages = len(self.newpagelist)
        try:
            self.log("new pages: %d" % (num_newpages))
            json_newpagelist = json.dumps(self.newpagelist)
        except Exception as e:
            self.log(e)
        self.newpagelist = {}

        # 'editsize'
        num_edits = len(self.editsizelist)
        try:
            self.log("edit size list: %d" % (num_edits))
            json_editsizelist = json.dumps(self.editsizelist)
        except Exception as e:
            self.log(e)
        self.editsizelist = {}

        # 'anonlist'
        num_anons = len(self.anonlist)
        try:
            self.log("num anons: %d" % (num_anons))
            json_anonlist = json.dumps(self.anonlist)
        except Exception as e:
            self.log(e)
        self.anonlist = {}

        # 'topicactivitylist'
        num_topics = len(self.topicactivitylist)
        # NOTE: this value may be filtered earlier to remove 'Talk' type articles

        try:
            self.log("num topics: %d" % (num_topics))
            json_topicactivitylist = json.dumps(self.topicactivitylist)
        except Exception as e:
            self.log(e)
        self.topicactivitylist = {}

        # 'useractivitylist'
        num_users = len(self.useractivitylist)
        try:
            self.log("num users: %d" % (num_users))
            json_useractivitylist = json.dumps(self.useractivitylist)
        except Exception as e:
            self.log(e)
        self.useractivitylist = {}


        ##########

        total_editsize = self.editsize
        num_robotedits = self.robotedits

        # reset other counters
        self.robotedits = 0
        self.editsize = 0
        self.newpages = 0

        self.log("************************************************************")

        # emit the number of events seen since last tick
        # TODO: this seems to be broken --> only the first two emits appear downstream
        #self.emit((num_events,), anchors=[])

        # send metrics to a database table
        connection = MySQLdb.connect(
                        host = self.options["db_host"],
                        user = self.options["db_user"],
                        passwd = self.options["db_pass"],
                        db = self.options["db_name"])
        db_table = self.options["db_table"]
        db_table_1 = self.options["db_table_1"]

        cursor = connection.cursor()

        try:
            self.log("***** Writing value to database *****")

            # Parameterize this insert statement as tuples for manageability
            schema = "(timestamp, eventcount, editsize, newpages, robotedits, numtopics, numusers)"
            sql = ("INSERT INTO %s %s VALUES (%%s, %%s, %%s, %%s, %%s, %%s, %%s)") % (db_table, schema)
            sql_param_values = (timestamp, num_events, total_editsize, num_newpages, num_robotedits, num_topics, num_users)
            cursor.execute(sql, sql_param_values)
            
            #self.log(schema)
            #self.log(sql)
            #log.debug(json_topiclist)
            #log.debug(json_userlist)

            # Parameterize this insert statement as tuples for manageability
            schema_1 = "(timestamp, topiclist, userlist, newpagelist, robotlist, editsizelist, anonlist, topicactivity, useractivity, nonbotuserlist)"
            sql_1 = "INSERT INTO %s %s VALUES (%%s, %%s, %%s, %%s, %%s, %%s, %%s, %%s, %%s, %%s)" % (db_table_1, schema_1)
            sql_param_values_1 = (timestamp, json_topiclist, json_userlist, json_newpagelist, json_robotlist, json_editsizelist, json_anonlist, json_topicactivitylist, json_useractivitylist, json_nonbotuserlist)

            if db_table_1:
                cursor.execute(sql_1, sql_param_values_1)


        except Exception as e:
            self.log("***** Database write error *****")
            self.log(sql)
            self.log(str(e))

            # TODO: consider adding another retry block in here
        finally:
            self.log("*****")
            cursor.close()
            connection.commit()
            connection.close()


if __name__ == '__main__':
    logging.basicConfig(
        level=logging.DEBUG,
        filename='/tmp/en-wikipedia-streaming_count_events.log',
        format="%(message)s",
        filemode='a',
    )

    CountEventsBolt().run()
