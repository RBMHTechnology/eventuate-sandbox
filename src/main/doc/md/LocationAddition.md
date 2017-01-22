### Location addition

Location addition requires additional rules as a new location may introduce a new cycle which may conflict with ongoing event deletion as outlined in _Cyclic replication networks and event deletion_ (see _Appendix_). For example, consider an acyclic replication network `ABC`:

```
A --- B
      |
      C
```

`A` emits events `e1`, `e2` and `e3` with `e1 → e2 → e3` which are replicated to `B` but not yet to `C`. Then `A` deletes events `e1` and `e2` which is allowed by the rules established so far. Now, location `D` is added 

```
A --- B
|     |
D --- C
```

and two bi-directional replication connections, `A - D` and `C - D`, are established. This may lead to a situation where `e3` is replicated from `A` to `D` and then to `C`. As a consequence, the causality filter at `C` will later reject events `e1` and `e2` from `B`.

Depending on the application this may or may not be acceptable. That is why if a replication connection is established the first time between two locations the application may chose to either replicate:

- the complete history,
- the current history or
- no history at all.

In general a location addition is defined as the first unidirectional replication connection from location `A1` to location `B1` (with events being replicated from `B1` to `A1`) from groups of locations `Ai` and `Bi` that are internally connected over unfiltered connections but have no replication connection from any `Aj` to any `Bj` yet. Note that the locations `Ai` and `Bi` can be connected to further locations over filtered connections, but these are not relevant for the considerations here and as cycles over filtered connections are not allowed they must never be connected once `A1` and `B1` are connected.

In case of a location addition depending on the required replication history (complete, current, no) a connection can be established as follows:

- Complete history: Event replication from location `B1` to location `A1` is only possible when `B1` has not deleted any event yet.
- Current history: Event replication starts with the first event in `B1`'s log.
- No history: When `B1` receives a connection request from `A1` it records a target replication progress of 0 for it to prevent further deletion of events and responds with its current sequence number. `A1` sets `B1`'s current sequence number as replication progress for `B1` and starts replicating events.

Further replication connections from any `Aj` to any `Bj` must always request the current history. Even if the initial connection requested only no history this does not mean that all events need to be replicated from `Bj` to `Aj`. It can actually be prevented that `Aj` receives events that causally precede any event received by `A1` (from `B1`). For this the replication connection may only be established after `CVV-Aj` causally succeeds these events (so they are filtered by the causality filter). Note that this conditions has to be ensured by the application (respectively an administrator) as there is no support in Eventuate.
  