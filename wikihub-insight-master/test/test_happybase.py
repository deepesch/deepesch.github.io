#!/usr/bin/env python
import happybase

conn = happybase.Connection('localhost')
conn.open()
wes = conn.table('wikieditssmall')
we = conn.table('wikiedits')

title = 'Afghanistan'

print "row"
years = range(2001,2015)
data_dict = {}
for y in years:
    rowval=wes.row(title+'_Y_'+str(y))
    if rowval is not None:
        print rowval
        val = rowval["count:edits"]
        if val is not None:
            print val

print "row scan"
data_dict2 = {}
for key, data in we.scan(row_prefix=title+'_Y_'):
    print key, data

prefix=title+'_Y_'
startrow="2011"
endrow="2015"

print "row scan"
data_dict3 = {}
for key, data in we.scan(row_start=prefix+startrow, row_stop=prefix+endrow):
    print key, data

print "row scan"
data_dict4 = {}
for key, data in we.scan(row_prefix='IPhone'+'_M_'):
    print key, data



conn.close()