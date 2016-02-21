import logging

from pyleus.storm import SimpleBolt

log = logging.getLogger('parser')

import json

class ParseEventBolt(SimpleBolt):

    # output types need to be of type 'list' or 'tuple' to be valid
    OUTPUT_FIELDS = ["json"]

    def process_tuple(self, tup):
       
        # extract the value from the StormTuple 
        line = tup.values

        '''
        This bolt partially serves as a placeholder to accomodate future design
        decisions with regard to the element responsible for IRC message parsing
        and the format of messages published by IRC logBot as a Kafka producer.

        We should re-evaluate if a namedtuple is a preferable container for data
        passed from IRC logBot sensors to Storm topologies for processing, and
        between Storm bolts.
        '''

        # convert the extract value to a JSON object
        json_line = json.loads(line[0])

        #log.debug(json_line["topic"])

        self.emit((json_line,), anchors=[tup])


if __name__ == '__main__':
    logging.basicConfig(
        level=logging.DEBUG,
        filename='/tmp/en-wikipedia-streaming_parse_event.log',
        format="%(message)s",
        filemode='a',
    )
    ParseEventBolt().run()
