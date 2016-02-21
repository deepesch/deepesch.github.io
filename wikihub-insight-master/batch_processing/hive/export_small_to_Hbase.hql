--export_tables_for_Hbase.hql
USE wiki;

drop table IF EXISTS editcountsmall;

CREATE EXTERNAL TABLE editcountsmall (HbaseKey STRING,
count BIGINT)
row format delimited
fields terminated by '\t'
stored as textfile
location '/user/ubuntu/for_Hbase/editcountsmall';

INSERT OVERWRITE TABLE editcountsmall
select CONCAT(title,'_',granularity,'_',year,month,day), editcount from editcount_small;