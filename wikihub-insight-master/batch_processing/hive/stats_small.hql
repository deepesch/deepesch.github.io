--stats.hql

USE wiki;

drop table IF EXISTS stats_small;

CREATE EXTERNAL TABLE stats_small (title STRING,
pid BIGINT,
ns SMALLINT,
unique_users BIGINT, 
unique_ids BIGINT, 
unique_ips BIGINT, 
total_edits BIGINT
)
row format delimited
fields terminated by '\t'
stored as textfile
location '/user/ubuntu/hive_tables/statssmall';

INSERT OVERWRITE TABLE stats_small
select title, pid, ns, count(distinct contributor_username) as unique_users, count(distinct contributor_id) as unique_ids, count(distinct contributor_ip) as unique_ips, count(rid) as total_edits
from wiki_small
where ns = 0
group by title, pid, ns
order by total_edits;