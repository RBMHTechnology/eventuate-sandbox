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

In general a primary location addition is defined as the first unidirectional replication connection from location `A1` to location `B1` (with events being replicated from `B1` to `A1`) from formerly unconnected groups of locations `Ai` and `Bi` that are internally connected over unfiltered connections. Note that the location `Ai` and `Bi` can be connected to further locations over filtered connections, but these are not relevant for the considerations here and as cycles over filtered connections are not allowed they must not never be connected once `A1` and `B1` are connected. As the groups were unconnected before the CVVs from `Ai` have no process ids in common with the CVVs from `Bi`.

In case of a primary location addition depending on the required replication history (complete, current, no) a connection can be established as follows:

- Complete history: Event replication from location `B1` to location `A1` starts if `DVV-B1` causally precedes or is equal to `CVV-A1` i.e. `DVV-B1 → CVV-A1` or `DVV-B1 = CVV-A1`. As `CVV-A1` and `DVV-B1` do not have any process ids in common this is equivalent to `DVV-B1` being zero, i.e. replication can only start if `B1` has not deleted any events yet.
- Current history: When `A1` establishes the connection to `B1` it sends its `CVV-A1`. `B1` records this as CVV of a directly connected location and with this prevents further deletion of events (as `CVV-A1` cannot meet the condition: `DVV-B1 → CVV-A1` or `DVV-B1 = CVV-A1`). `B1` responds with its `DVV-B1` and `A1` merges this into its `CVV-A1` and `DVV-A1` so that its log is in the same state as if all events deleted from `B1` where replicated and afterwards deleted locally.
- No history: Similarly to the replication of the current history, `A1` sends its `CVV-A1` and `B1` records this as CVV of a directly connected location to prevent further deletion of events, but `B1` responds with its `CVV-B1` and `A1` merges this into its `CVV-A1` and `DVV-A1` so that its log is in the same state as if all events from `B1` where replicated and afterwards deleted.

Further replication connections from any `Aj` to any `Bj` must always request the complete history. As in this case `Aj` potentially received `B`-events already (via `A1`), `DVV-Bj` is not required to be zero for the replication to start, but the more general condition for complete history noted above must hold. Even if the initial connection requested only current or no history this does not mean that all events need to be replicated from `Bj` to `Aj`. It can actually be prevented that `Aj` receives events that causally precede any event received by `A1` (from `B1`). For this the replication connection may only be established after `CVV-Aj` causally succeeds these events (so they are filtered by the causality filter). Note that this conditions has to be ensured by the application (respectively an administrator) as there is no support in Eventuate.
 
If the primary location addition chooses to replicate the complete history, the order of the location addition and further replication connections actually does not matter. Only connections to locations without deleted events start the replication while the others wait until their CVV causally succeeds the DVVs of the locations they connect to. In the example given above `D` may first connect to `A`. In that case replication will not start as `DVV-A` does not precede `CVV-D`, but further deletion on `A` is prevented. If `D` connects to `C` afterwards, replication begins immediately and once `CVV-D` succeeds `DVV-A`, the replication from `A` also begins.
 
A secondary location addition is defined as the first unidirectional replication connection from location `B1` to `A1` (with events being replicated from `A1` to `B1`) from groups of locations `Ai` and `Bi` where at least two locations `Aj` and `Bj` are already connected in the other direction. Note that the secondary location addition must not introduce replication cycles over filtered connections (except in case of redundant filtered connections). As events are already being replicated from group B to group A, the CVVs from `Ai` contain process ids from `Bi` while the CVVs from `Bi` do not contain any process ids from `Ai`. Due to this establishing a connection with current or no replication history works slightly different than in case of primary location addition:

When `B1` establishes the connection to `A1` as before it sends its `CVV-B1`, `A1` records this as CVV of a directly connected location and responds with its `DVV-A1` (in case of current history) or `CVV-A` (in case of no history) . As this version vector might already contain process ids from `B`, `B1` only merges a projection of this version vector containing only process ids not yet in `CVV-B1` into its `CVV-B1` and `DVV-B1`.  Note that it is up to the application (respectively an administrator) to ensure that `CVV-B1` already contains all process ids from `B` so that merging `CVV-A1` does not affect process ids that are used by any `B` location.
