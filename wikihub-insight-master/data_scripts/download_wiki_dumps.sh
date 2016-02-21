#!/bin/bash
# Set script to fail on error
set -e
# Fail on unref var
set -u
# GET THE DATE
DATESTR=$(date '+%Y%m%d')
FOLDERNAME=wiki$DATESTR
echo $FOLDERNAME
if [ -e $FOLDERNAME ] ; then
	rm -rf $FOLDERNAME;    
fi
mkdir -p $FOLDERNAME;
cd $FOLDERNAME
wget http://dumps.wikimedia.org/enwiki/latest/enwiki-latest-stub-meta-history{1..27}.xml.gz

