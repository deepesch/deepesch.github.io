--Drop Tables.hql
USE wiki;

drop table IF EXISTS wikiedits;
drop table IF EXISTS wikieditslatest;
drop table IF EXISTS wiki_small;
drop table IF EXISTS wiki_smalllatest;
drop table IF EXISTS ag_all;
drop table IF EXISTS ag_small;
drop table IF EXISTS editcount;
drop table IF EXISTS editcountsmall;

DROP DATABASE IF EXISTS wiki;