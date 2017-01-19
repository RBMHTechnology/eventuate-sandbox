### Disaster recovery

A disaster is defined as total or partial event loss at a given location. In contrast to event deletion, event loss always starts **from** a given position in a local event log  (see also section Disaster recovery in the user documentation). Goal of disaster recovery is to recover local event log state from directly connected locations in the replication network. Disaster recovery has a mandatory _metadata recovery_ phase and an optional _event recovery_ phase.
#### Metadata recovery

With a disaster, not only events but also the local time of the local vector clock is lost. Without properly recovering the clock, it may start running again in the past i.e. at a time before the disaster happened. This may lead to local time values that are perceived as duplicates in the replication network which must be prevented because it breaks causality and interferes with causality filters. 

Therefore, during metadata recovery, the local time of the local vector clock must be set to maximum of values seen at all other directly connected locations. Furthermore, the replication progress, recorded at directly connected locations, must be reset to the sequence number of the last event in the local event log or to zero if the local event log is completely lost. 
#### Event recovery

Event recovery means replicating events back from locations that are directly connected to the location to be recovered. These directly connected locations must have unfiltered bi-directional replication connections with the location to be recovered. For example, a location `A` can only be recovered from a location `B` if both replication connections `A -> B` and `A <- B` are unfiltered.

Event recovery over filtered connections is only supported for _terminal locations_ i.e. locations that are connected to only one other location. When replicating events to the terminal location during recovery, application-specific replication filters must be OR-ed with a filter that accepts events that have been initially emitted at that terminal location.

Restrictions also apply to event recovery from locations with deleted events. Assuming that location `A` should be recovered from location `B`, event recovery is only allowed if the deletion version vector of `B` causally precedes or is equal to the current version vector of `A` i.e. `DVV-B → CVV-A` or `DVV-B = CVV-A`.

Assuming that location `A` can be recovered from locations `B` and `C` but only `B` meets this condition, a first round of event recovery must be attempted from `B`. After recovery from `B` has completed, `CVV-A` has an updated value and the condition must be evaluated for `C` again. If it is met, event recovery must be attempted from `C` in a second round. 

**Hint:** Applications that want to delete events older than n days from their local event logs should consider local event log backups at intervals of m days with m < n in order to meet the conditions for event recovery from locations with deleted events.
