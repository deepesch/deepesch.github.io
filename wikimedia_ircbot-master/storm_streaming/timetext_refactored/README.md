We refactored this topology heavily. It is configurable to run user-specified time intervals using 'tick tuples'. Make a new topology .yaml file for each interval of interest. Remember to assign a unique topology name, zookeeper storage location, kafka consumer id, and output database tables for each configuration. A test topology can be used to regression test changes locally prior to production deployment.

***
### Notes:
- We use the Pyleus `shuffle_grouping` Storm topology setting, with `parallelism` of one (1) as a functional equivalent of a `direct_grouping`, as Pyleus does not support `direct_grouping`
  - See: https://github.com/Yelp/pyleus/issues/12

***

### TODO:
- Revise topology architecture to be more memory efficient, with regard to counters and buffers between intervals and activity tracking. Integrate HBase/Cassandra storage of streaming activity.
- Optimize + Refactor
- Sliding Windows?
- Dedupe?
- Replay rejection?
