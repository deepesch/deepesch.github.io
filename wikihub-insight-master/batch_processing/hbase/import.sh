#!/bin/bash
hbase org.apache.hadoop.hbase.mapreduce.ImportTsv -Dimporttsv.columns=HBASE_ROW_KEY,count:edits wikieditssmall '/user/ubuntu/for_Hbase/editcountsmall'
hbase org.apache.hadoop.hbase.mapreduce.ImportTsv -Dimporttsv.columns=HBASE_ROW_KEY,count:edits wikiedits '/user/ubuntu/for_Hbase/editcount'
