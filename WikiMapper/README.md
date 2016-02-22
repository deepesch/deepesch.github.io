# wikimedia_ircbot
Insight Data Science - Data Engineering Fellows Program 2015

***

I love to learn, so I spend a lot of time on Wikipedia. It's a pretty special place -- Thanks to the internet, we have the sum of the world's knowledge, available to anyone, anywhere for free. Each month, over 500 million unique visitors, come to read from the 5.5 million articles curated in 280 different languages.

These articles are authored and maintained by a global community of around 250,000 registered users -- And what's really special about this is that they're all volunteers.

Wikipedia editors care enough about these subjects that they volunteer their time to do this; they're influencing the world and what we know about it -- That's worth studying.

Each day, around 25,000 editors are actively contributing to Wikipedia. If you were interested in what they were writing about today, or yesterday, you might have to wait up to thirty days to find out.

Right now Wikipedia database dumps are available on a monthly basis, and this project aims to solve that problem.

***

A real-time processing data pipeline proof of concept using Apache Kafka, Storm, and Hadoop. wikimedia_ircbot is an IRC bot created for tracking Wikimedia (e.g. en.wikipedia.org) edits.

The code in this project is extensively commented and intended to provide examples for how these technologies can be used to solve various aspects of data engineering problems. There are always areas to improve on design and implementation.
- Check the repository [`wiki`](https://github.com/moandcompany/wikimedia_ircbot/wiki) for more information and commentary on the technologies used
- Please feel free to provide feedback, suggestions, or contribute some code.
- Generally, each section of this project will also contain a `README` discussing the technologies used

![insight week 4 mod 4 003](https://cloud.githubusercontent.com/assets/1381775/6098334/23bf3544-af90-11e4-9546-5304294774cd.jpg)


Read more about this project:
- http://www.slideshare.net/moandcompany/insight-data-engineering-week-4-andrew-mo

View some telemetry examples:
- http://wikiwatch.andrewmo.com  [ project servers have since been taken down ]

***

I normally work as a project manager / solutions engineer, so I decided to treat this project as an initial agile sprint.

Please feel free to write back with features or metrics you would be interested in.

***

![insight week 4 mod 4 005](https://cloud.githubusercontent.com/assets/1381775/6098336/2c0f62b4-af90-11e4-8c2c-e9fa9019949f.jpg)

### Wikimedia IRC Bot:
- Logs Wikimedia IRC channels to files and/or related Kafka Broker topics
- Configurable to monitor a configurable set of channels

### Wikimedia IRC Message Parser:
- Parses IRC channel messages into compact tuples and/or extended JSON with ISO8601/RFC3339 timestamps
- Can be used with Wikimedia IRC Bot, Stream, and Batch processing layers

### Wikimedia Bot Stream Processor:
- Processes Kafka topics containing Wikimedia IRC channel messages
- Computes interval/window-based channel activity summaries

### Wikimedia Batch Processor:
- Computes minute, hourly, and daily metrics using IRC Bot logs
- Loads logged event stream data to HBase tables

***

### Capture and Ingest

![insight week 4 mod 4 006](https://cloud.githubusercontent.com/assets/1381775/6098369/f93df110-af90-11e4-8616-a96323e37514.jpg)

![insight week 4 mod 4 007](https://cloud.githubusercontent.com/assets/1381775/6098337/3255a70a-af90-11e4-9174-8fefde9ebac6.jpg)

![insight week 4 mod 4 015](https://cloud.githubusercontent.com/assets/1381775/6098338/3c1a39cc-af90-11e4-97ba-3787c3142b95.jpg)

***

### Related:
- RESTful API Endpoint:
  - https://github.com/moandcompany/wikimedia_ircbot_apiserver

### Building:
- Wikimedia IRC Bot:  No build Required (Python 2.6+)
- Wikimedia IRC Message Parser:  No build Required (Python 2.6+)

### Dependencies:
- Happybase 0.9 (HBase with Python)
- Kafka-Python 0.9.2 (Kafka with Python)
- Pyleus 0.2.4 (Storm with Python)
- Twisted 14.0.2 (Sockets with Python)

### Services Used:
- Apache Hadoop 2.6.0.2.2.0.0	
- Apache HBase 0.98.4.2.2.0.0	
- Apache Hive 0.14.0.2.2.0.0
- Apache Kafka 0.8.1.2.2.0.0
- Apache Pig 0.14.0.2.2.0.0	
- Apache Storm 0.9.3.2.2.0.0
- Apache Tez 0.5.2.2.2.0.0	
- Apache Zookeeper 3.4.6.2.2.0.0	

***

### Guiding Principles:
- Keep it Simple.
- Simplify, Simplify.
- Common, reusable codebase.
- Python everywhere possible.
- Have Fun.
- ![nom](http://i.imgur.com/0B3dbH6.gif)
- Use loosely-coupled components with high integration value.

### Fun stuff encountered along the way:
- Authored a step-by-step install guide to use Cloudera HUE 3.7+ with HDP 2.2
  - See: http://gethue.com/hadoop-hue-3-on-hdp-installation-tutorial/ 
- Spark Streaming and Scala. Maybe next sprint.
- Scala Build Tool (SBT) build dependency nightmares with Apache Spark (Scala)
  - Spark Streaming (Scala) doesn't natively include the `org.apache.spark.streaming.kafka` class when a Spark Streaming package is submitted, which means a 'fat jar' including this class is required; with SBT, this is done using `sbt assembly`, and Spark / Spark Streaming is extremely prone to duplication errors (i.e. packages with the same name, but differing in version) during dependency resolution and merging. You may spend hours hunting down duplicate dependencies to exclude before it works. The best part of this process is that SBT doesn't tell you all of the duplicates you need to resolve for a successful merge, so this may take several iterations. 
    - Example: https://github.com/moandcompany/wikimedia_ircbot/blob/master/spark_streaming/build.sbt
  - ![Resolving an SBT Merge Issue](http://i.imgur.com/t0XHtgJ.gif)
  - Maybe things are easier with Maven?
- Pyleus class casting bug and workaround:
  - https://github.com/Yelp/pyleus/issues/93
  - https://github.com/Yelp/pyleus/pull/94
- Pyleus topology building notes:
  - https://github.com/Yelp/pyleus/issues/95
- The need for a complete Pyleus - Kafka Spout example topology:
  - https://github.com/Yelp/pyleus/issues/40
- Storm / Python testing:
  - Never, ever, ever use `print` statements in your Storm code for debugging, unless you hate yourself and enjoy spending hours wondering why your topologies stop working without a stacktrace for insight; use the Python `logging` module instead.
    - The Storm Multi-Lang Protocol that helps make all of this great stuff work uses `STDIN` and `STDOUT` for inter-task communication. (See https://storm.apache.org/documentation/Multilang-protocol.html) 
- QA: Always, always check your code for references to `localhost`
    - Every wonder why your code works locally, but not in a distributed system? ^_^

### Alternatives to Consider:
- Streamparse v Pyleus
  - Streamparse requires Python 2.7+, while Pyleus requires Python 2.6+
  - Could be significant for RHEL/CentOS 6 environments (requires Python 2.6 for YUM) if Virtualenv is inconvenient
- Pulsar v Samza v Spark Streaming v Storm
  - Samza introduces stream processing as a native YARN application
  - Spark 1.2 introduces Spark Streaming support for Kafka, however these features are not fully available for Python
- Accumulo v Cassandra v HBase
  - HBase is a native member of the Hadoop ecosystem, and can be scaled with the core Hadoop cluster; relies upon HBase Thrift server for use with Python/Happybase, a potential performance bottleneck
  - Cassandra offers a 'masterless' architecture and the 'CQL' query language
  - Accumulo has included cell-level access control from inception (HBase has cell-level access control too) 

### Upcoming Technologies of Interest:
- Kafka on YARN (KOYA)
- Kafka Security (including Authentication)

### Recommendations:
- Consider using HDP (Hortonworks Data Platform) or CDH (Cloudera's Distribution including Apache Hadoop) to have a common service management layer using Apache Ambari or Cloudera Manager; Storm and Kafka are built-in components for HDP 2.2

### Topology Strategies:
- Storm Topology persistence is great!
- Design different topologies for different metrics or time windows
- Parameterized base topologies facilitate configuration management 
- Remember to specify unique topology names and Kafka consumer IDs

### TODO:
- Improve automation of topology configuration, deployment, and exposure
- Evaluate Storm Trident
- Add 'watchword' traps and signals
- Fix IRC logBot logrotation behavior
- Improve IRC logBot Kafka error handling and HA modes
- Reevaluate IRC logBot data sourcing implementation based on communication with Wikimedia Foundation members
- Incorporate metrics from academic research on Wikipedia user edit activity (e.g. 'edit sessions')
- Incorporate invalid replay rejection methods
- Incorporate sensor diversity and deduplication methods
- Incorporate sensor persistence and fault recovery methods
- Investigate tick-tuple epoch synchronization across topologies vs All Grouping
- Add context enrichment using Wikipedia API and other tools (e.g Pattern)
- Continue implementation of NoSQL storage from processing topology (Apache Cassandra / HBase)
- GeoIP lookup for anonymous users
- Continue evaluation of `Tez` as an alternative execution for MapReduce based applications (e.g. `Pig`)
- Evaluate `Pulsar` as an alternative stream processing engine

***

![insight week 4 mod 4 016](https://cloud.githubusercontent.com/assets/1381775/6098392/880759a4-af91-11e4-9c67-63a4bff6d39a.jpg)
