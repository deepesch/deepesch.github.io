-- Calculate the total number of edits and net size change per day

A = LOAD '$input_dir' USING PigStorage('\t') AS (ts:datetime, topic:chararray, url:chararray, user:chararray, editsize:int, flags:chararray, comment:chararray);

--- Deduplicate source log data; use parallelism to mitigate chance of reducer failure
unique = DISTINCT A PARALLEL 2;


-- Input data is marked to the second, truncate date/time values to day
temp = FOREACH unique GENERATE CONCAT((chararray)ToString(ts, 'yyyy-MM-dd\'T\''), '00:00:00Z') AS ts, topic, user, editsize;

B = FOREACH temp GENERATE FLATTEN($0) AS eventtime, topic, user, editsize;

C = group B by eventtime;

 -- Bin each time interval (day) with number of events, net edit size, number of unique topics and users
D = FOREACH C {
    unique_topics = DISTINCT B.topic;
    unique_users = DISTINCT B.user;
    editsize = SUM(B.editsize);
    GENERATE group, COUNT(B), COUNT(unique_topics) as topic_count, COUNT(unique_users) as user_count, editsize;
};

-- Sort output by date
sorted = ORDER D by $0 ASC;

-- dump sorted;

STORE sorted INTO '$output_path/$output_folder';
