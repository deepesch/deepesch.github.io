-- Calculate the total number of edits, contributing users,  and net size change per topic, per day

A = LOAD '$input_dir' USING PigStorage('\t') AS (ts:datetime, topic:chararray, url:chararray, user:chararray, editsize:int, flags:chararray, comment:chararray);

--- Deduplicate source log data; use parallelism to mitigate chance of reducer failure
unique = DISTINCT A PARALLEL 2;

-- Input data is marked to the second, truncate date/time values to day
temp = FOREACH unique GENERATE CONCAT((chararray)ToString(ts, 'yyyy-MM-dd\'T\''), '00:00:00Z') AS ts, topic, user, editsize;

B = FOREACH temp GENERATE FLATTEN($0) AS eventtime, topic, user, editsize;

C = group B by (eventtime, topic);

-- Bin each time interval (day) with number of events, net edit size, number of unique topics and users
D = FOREACH C {
    unique_users = DISTINCT B.user;
    net_editsize = SUM(B.editsize);
    GENERATE FLATTEN(group), COUNT(B), COUNT(unique_users) as user_count, net_editsize;
};

temp2 = group D by eventtime;
temp3 = FOREACH temp2 {
    result = TOP(10, 4, D);
    GENERATE FLATTEN(result);
};

-- Sort output by date (chronologically), then by topic count in descending order
sorted = ORDER temp3 by $0 ASC, $4 DESC;

STORE sorted INTO '$output_path/$output_folder';
