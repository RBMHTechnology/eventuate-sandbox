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

To prevent this anomaly, further rules must be defined for event replication over new replication connections. This doesn't only apply to connections to and from new locations but also to new replication connections between existing locations: event replication over a new replication connection from location `X` to location `Y` may only start if the deletion version vector of `X` (`DVV-X`) causally precedes or is equal to the current version vector of `Y` (`CVV-Y`) i.e. `DVV-X → CVV-Y` or `DVV-X = CVV-Y` 

Applying this rule to the above example, replication between `C` and `D` would start immediately in both directions and between `A` and `D` only in direction from `D` to `A`. Replication from `A` to `D` only starts after `D` received events `e1` and `e2` from `C`, preventing the anomaly that `C` rejects events `e1` and `e2`.

For the special case that location `D` is not even interested in getting all past events from `A` and `C` but only wants to receive new events, it must first set both its `CVV` and its `DVV` to the least upper bound of the `CVV`s of locations `A` and `C`. More generally, a new location `X` that only wants to receive new events from all new directly connected locations `Y1` - `Yn` must additionally set its `CVV-X` and `DVV-X` to the `LUB(DVV-Y1, ..., DVV-Yn)`, before applying the previous rule.
