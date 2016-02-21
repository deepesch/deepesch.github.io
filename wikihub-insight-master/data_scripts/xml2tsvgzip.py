#!/usr/bin/env python
import os
import fnmatch
from os import listdir
from os.path import isfile, join
import sys
#import time
import datetime
import re
import gzip
import xml.sax
from xml.sax import handler

# Define XML parser handler
class WikiHandler(handler.ContentHandler):
    def __init__(self,fhandle):
        """ The class constructor accepts a file handle to flush out data to disk
        """
        self.CurrentData = ""
        self.fout = fhandle
        self.page_data = {}
        self.revision_data = {}
        self.in_page = False
        self.in_rev = False
        self.in_contributor = False
        self.minor = ""
        self.ignored_tags = ["model","format"]
        self.key_order_page = ["title","pid","ns","redirect_title"]
        self.key_order_rev = ["rid","parent_id","timestamp","unix_ts","contributor_username","contributor_id","contributor_ip","minor","comment","comment_deleted","text_id","text_bytes","text_deleted","sha1"]
    

    
    # Call StartElement
    def startElement(self, tag, attributes):
        self.CurrentData = tag
        if tag == "page":
            self.in_page = True
        elif tag == "redirect":
            self.page_data["redirect_title"] = attributes["title"]
        elif tag == "revision":
            self.in_rev = True
        elif tag == "text":
            #sys.stderr.write(str(attributes.getNames()))
            for key, val in attributes.items():
                self.revision_data["text_"+key] = val
        elif tag == "comment":
            if "deleted" in attributes.keys():
                self.revision_data["comment_deleted"] = "1"
        elif tag == "contributor":
            self.in_contributor = True
        elif tag == "minor":
            self.revision_data["minor"] = "1"
        else:
            pass
            
    # End
    def endElement(self, tag):
        self.CurrentData = tag
        if self.CurrentData == "contributor":
            self.in_contributor = False
        elif self.CurrentData == "revision":
            # Flush to file
            page_fields = [self.page_data.get(ks,"") for ks in self.key_order_page]
            revision_fields = [self.revision_data.get(ks2,"") for ks2 in self.key_order_rev]
            record = "\t".join(page_fields + revision_fields)
            self.fout.write(record.encode('ascii','ignore'))
            self.fout.write("\n")
            self.revision_data = {}
            self.minor = ""
            self.in_rev = False
        elif self.CurrentData == "page":
            self.page_data = {}
            self.in_page = False
        else:
            pass
        self.CurrentData = ""

    # characters
    def characters(self, content):
        if self.CurrentData == "title":
            self.page_data["title"] = content
        elif self.CurrentData == "ns":
            self.page_data["ns"] = content
        elif self.CurrentData == "parentid":
            self.page_data["parent_id"] = content
        elif self.CurrentData == "id":
            if self.in_contributor:
                self.revision_data["contributor_id"] = str(content)
            elif self.in_rev:
                self.revision_data["rid"] = str(content)
            elif self.in_page:
                self.page_data["pid"] = str(content)
        elif self.CurrentData == "timestamp":
            self.revision_data["timestamp"] = content
            dtobj = datetime.datetime.strptime(content, "%Y-%m-%dT%H:%M:%SZ")
            self.revision_data["unix_ts"] = str(int(unix_time(dtobj)))
        elif self.CurrentData == "username":
            self.revision_data["contributor_username"] = content
            self.revision_data["contributor_ip"] = ""
        elif self.CurrentData == "ip":
            self.revision_data["contributor_username"] = content
            self.revision_data["contributor_ip"] = content
            self.revision_data["contributor_id"] = content
        elif self.CurrentData == "comment":
            self.revision_data["comment"] = content
        elif self.CurrentData == "sha1":
            self.revision_data["sha1"] = content
        elif self.CurrentData in ["model","format","redirect","revision","contributor","text"]:
            pass
        else:
            pass

def unix_time(dt):
    epoch = datetime.datetime.utcfromtimestamp(0)
    delta = dt - epoch
    return delta.total_seconds()

# Start of the main function
def main():
    # wkh = WikiHandler()
    # incparser.setContentHandler(wkh)
    # filename = 'enwiki-20121101-stub-meta-history1.xml.gz'
    for filename in fnmatch.filter(os.listdir('.'), 'enwiki-*-stub-meta-history?*.xml.gz'):
        basefilename = os.path.splitext(os.path.splitext(filename)[0])[0]
        foutname = basefilename+".txt.gz"
        with gzip.open(filename, 'rb') as f:
            with gzip.open(foutname, 'wb') as fout:
                incparser = xml.sax.make_parser()
                wkh = WikiHandler(fout)
                incparser.setContentHandler(wkh)
                for line in f:
                    if line is not None:
                        sline = line.strip()
                        incparser.feed(sline)
                incparser.close()
                # page_fields = [wkh.page_data.get(ks,"") for ks in key_order_page]
                # revision_fields = [revision_data.get(ks2,"") for ks2 in key_order_rev]
                # record = "||".join(page_fields + revision_fields)
                # print record.encode('ascii','ignore')
    return


if __name__ == '__main__':
    main()