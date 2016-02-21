--
set hive.map.aggr=true;
-- 
USE wiki;

drop table IF EXISTS rev_all;
drop table IF EXISTS editcount_all;

create table rev_all
as select title, pid, ns, SUBSTR(ts,0,4) as year, SUBSTR(ts,6,2) as month, SUBSTR(ts,9,2) as day, rid
from wikiedits
where ns =0;

create table editcount_all
as select * 
from
(select title, pid, 'D' as granularity, year, month, day, count(rid) as editcount 
 from rev_all
 group by title, pid, year, month, day
 UNION ALL
 select title, pid, 'M' as granularity, year, month, '' as day, count(rid) as editcount 
 from rev_all
 group by title, pid, year, month
 UNION ALL
 select title, pid, 'Y' as granularity, year, '' as month, '' as day, count(rid) as editcount 
 from rev_all
 group by title, pid, year ) t;

--