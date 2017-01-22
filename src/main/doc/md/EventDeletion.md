### Event deletion

Event deletion distinguishes logical deletion from physical deletion as explained in [Deleting events](http://rbmhtechnology.github.io/eventuate/reference/event-log.html#deleting-events). Event deletion is always performed **up to** a given position in a local event log.

Events may be deleted from a local event log `A` only if for all local event logs `i` that are directly connected to `A` the replication progress passed the sequence number of the event to be deleted. In other words, all events that should be physically deleted from a log `A` must have already been replicated to all directly connected logs `i`. This does not only ensure that all events are replicated to directly connected event logs but also prevents the replication anomaly described in _Cyclic replication networks and event deletion_ (see _Appendix_).

For this event log `A` records the target replication progresses of all directly connected logs locally. They are updated with each replication read request from `i`.