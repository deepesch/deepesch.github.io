-- Calculate the total number of edits and net size change per minute

A = LOAD '/user/ubuntu/enwikipedia_logs' USING PigStorage('\t') AS (ts:datetime, topic:chararray, url:chararray, user:chararray, editsize:int, flags:chararray, comment:chararray);

--- Test Dedupe
dedupe = DISTINCT A;

-- Input data is marked to the second, truncate date/time values to minute of day
temp = FOREACH dedupe GENERATE CONCAT((chararray)ToString(ts, 'yyyy-MM-dd\'T\'HH:mm'), ':00Z') AS ts, editsize;

B = FOREACH temp GENERATE FLATTEN($0) AS eventtime, editsize;

C = group B by eventtime;

-- Bin each time interval (hour of day) with number of events and net editsize
D = FOREACH C GENERATE group, COUNT(B), SUM(B.editsize);

-- Sort output by date/time
sorted = ORDER D by $0 ASC;

dump sorted;
