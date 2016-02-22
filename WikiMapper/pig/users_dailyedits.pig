-- Calculate the total number of edits and net size change per user, per day

A = LOAD '$input_dir' USING PigStorage('\t') AS (ts:datetime, topic:chararray, url:chararray, user:chararray, editsize:int, flags:chararray, comment:chararray);

--- Deduplicate source log data; use parallelism to mitigate chance of reducer failure
unique = DISTINCT A PARALLEL 2;

-- Input data is marked to the second, truncate date/time values to day
temp = FOREACH unique GENERATE CONCAT((chararray)ToString(ts, 'yyyy-MM-dd\'T\''), '00:00:00Z') AS ts, user, editsize;

B = FOREACH temp GENERATE FLATTEN($0) AS eventtime, user, editsize;

C = group B by (eventtime, user);

-- Bin each time interval (day), for each user, with number of events and net editsize
D = FOREACH C GENERATE FLATTEN(group), COUNT(B), SUM(B.editsize);

-- Sort output by date
sorted = ORDER D by $0, $1 ASC;

STORE sorted INTO '$output_path/$output_folder';
