--export_alltable_for_Hbase.hql
USE wiki;

drop table IF EXISTS editcount;


CREATE EXTERNAL TABLE editcount (HbaseKey STRING,
count BIGINT)
row format delimited
fields terminated by '\t'
stored as textfile
location '/user/ubuntu/for_Hbase/editcount';

INSERT OVERWRITE TABLE editcount
select CONCAT(title,'_',granularity,'_',year,month,day), editcount from editcount_all;
