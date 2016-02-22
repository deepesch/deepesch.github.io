# Apache Storm

***

## 'The Hadoop of Real-time'

`Apache Storm`, authored by Nathan Marz, provides a real-time data processing framework, with sub-millisecond latency.

Storm is inherently a parallel processing technology that runs across a cluster of commodity machines, providing economical `scalability` and `fault-tolerance` / `high-availability`. For these reasons, `Storm` is often called the "`Hadoop` of real-time," however it is important to note that `Storm` and `Hadoop` address different problems, and unlike `Hadoop`, `Storm` does not provide a storage layer.

The smallest unit of data processed by `Storm` is a `Tuple`, which can logically be considered as a representation of an event or message. Processing of a `Tuple`, or stream of tuples, is described using a `Topology`; a `Topology` is a directed, acyclic graph that specifies the relationship between a data source, called a `Spout`, and processing steps or actions to be performed on the data; processing steps are encapsulated in modules called `Bolts`.

`Storm` clusters include a `Nimbus` master node, to submit and manage `Topologies`, and `Supervisors` that execute tasks described in those topologies. A `Storm` cluster uses the `Zookeeper` service to manage coordination between the `Nimbus` and `Supervisors`.

`Storm` has been designed from the ground up to ensure availability and that all `tuples` are processed `at-least-once`; `tuples` will be replayed through a `topology` if positive acknowledgement that the data has been processed by all topology steps is not received.

`Storm` clusters are highly fault tolerant as `Topologies` will continue to run, even if the cluster `Nimbus` is disabled. `Tasks` are automatically restarted in the event that a `Supervisor` fails to perform its duties. Note: `Storm` reliability requires that we have a reliable `Zookeeper` quorum; for this project, we used a Zookeeper quorum of size three (3).

`At-most-once` processing use cases can be supported using `Storm Trident`, if needed, which introduces a `micro-batch` processing approach to `Storm`.

See:
- http://nathanmarz.com
- https://storm.apache.org/about/


***

TODO:
- `Storm Trident` re-evaluation
