#!/usr/bin/env python
import happybase
import time
from datetime import datetime, date
import calendar
import pandas

def convertDateKeys(data_dict, JS = True):
    data_dict_cv = {}
    cvfactor=1
    if JS:
        cvfactor=1000
    for k, v in data_dict.items():
        if len(k)==4:
            new_key = cvfactor*date_string_to_ts(k,"Y")
        elif len(k)==6:
            new_key = cvfactor*date_string_to_ts(k,"M")
        elif len(k)==8:
            new_key = cvfactor*date_string_to_ts(k,"D")
        else:
            new_key = k
        data_dict_cv[new_key]=v
    return data_dict_cv


def getPoint(query_string):
    conn = happybase.Connection('localhost')
    conn.open()
    wes = conn.table('wikieditssmall')
    we = conn.table('wikiedits')
    rowval=we.row(title+'_Y_'+year)
    if rowval is not None and rowval != {}:
            val = rowval["count:edits"]
            if val is not None:
                datapoint = val
                conn.close()
                return val
    conn.close()
    return None

def getYearlyData(title, yrange = ("2001","2014")):
    conn = happybase.Connection('localhost')
    conn.open()
    wes = conn.table('wikieditssmall')
    we = conn.table('wikiedits')
    titlestr = title.encode('ascii','ignore')
    years = range(2001,2015)
    data_dict = {}
    for y in years:
        year=str(y)
        rowval=we.row(title+'_Y_'+year)
        if rowval is not None and rowval != {}:
                val = rowval["count:edits"]
                if val is not None:
                    data_dict[y] = int(val)
    conn.close()
    return data_dict

def date_string_to_ts(date_str,granularity,startperiod = True):
    # Number of days in each month
    modaynum = {'01': 31, '02': 28, '03': 31, '04': 30, '05': 31, '06': 30, '07': 31, '08': 31, '09': 30, '10': 31, '11': 30, '12': 31}
    if granularity not in ("Y","M","D"):
        return None
    elif granularity=="D":
        d=date(int(date_str[:4]),int(date_str[4:6]),int(date_str[6:8]))
    elif granularity=="M":
        if startperiod:
            d=date(int(date_str[:4]),int(date_str[4:6]),1)
        else:
            d=date(int(date_str[:4]),int(date_str[4:6]),modaynum[date_str[4:6]])
    elif granularity=="Y":
        if startperiod:
            d=date(int(date_str[:4]),1,1)
        else:
            d=date(int(date_str[:4]),12,31)
    return calendar.timegm(d.timetuple()) 

def getData(title, time_granularity = "Y"):
    start_t = time.time()
    conn = happybase.Connection('localhost')
    conn.open()
    wes = conn.table('wikieditssmall')
    we = conn.table('wikiedits')
    titlestr = title.encode('ascii','ignore')
    data_dict = {}
    for key, data in we.scan(row_prefix=title+'_'+time_granularity+'_'):
        dict_key = key.replace(title+'_'+time_granularity+'_','')
        data_dict[dict_key] = int(data["count:edits"])
    conn.close()
    end_t = time.time()
    elapsed_t = start_t-end_t
    return data_dict

def getRangedData(title, time_granularity = "Y", start="2001", end="2014", dates_to_epoch = True):
    #startrow = parseDatePart(start,time_granularity)
    #endrow = parseDatePart(end,time_granularity)
    startrow = start.replace('-','')
    endrow = end.replace('-','')
    endrow_int = int(endrow)
    endrow_int +=1
    endrow=str(endrow_int)
    start_t = time.time()
    conn = happybase.Connection('localhost')
    conn.open()
    wes = conn.table('wikieditssmall')
    we = conn.table('wikiedits')
    titlestr = title.encode('ascii','ignore')
    prefix=title+'_'+time_granularity+'_'
    data_dict = {}
    for key, data in we.scan(row_start=prefix+startrow, row_stop=prefix+endrow):
        dict_key = key.replace(prefix,'')
        data_dict[dict_key] = int(data["count:edits"])
    conn.close()
    end_t = time.time()
    elapsed_t = start_t-end_t
    return data_dict

# def getStats(title):
#     conn = happybase.Connection('localhost')
#     conn.open()
#     wes = conn.table('stats_small')
#     we = conn.table('stats_all')
#     titlestr = title.encode('ascii','ignore')
#     years = range(2001,2012)
#     data_dict = {}
#     for y in years:
#         year=str(y)
#         rowval=wes.row(title+'_'+year)
#         if rowval is not None:
#             if rowval != {}:
#                 val = rowval["count:edits"]
#                 if val is not None:
#                     data_dict[y] = val
#     conn.close()
#     return data_dict
