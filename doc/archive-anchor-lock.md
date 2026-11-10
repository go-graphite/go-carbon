# Archive anchor publication

Anchor repair reads a committed view and prepares complete classic-ring
replacement groups before acquiring the metric's mutation stripe. It preserves
every physical timestamp and floating-point bit, including NaN payloads and
signed zero. An ambiguous archive blocks the whole metric.

Publication previously reread the catalog and revision under that stripe.
Those two logical reads can require several cold table reads. A 581-point
archive with 562 misplaced points is therefore enough to reproduce a long
hold independently of rewrite CPU or archive size.

Every mutation now advances an atomic epoch at its beginning and end, under
its existing stripe. This also covers uncached stores, revision-neutral
maintenance, expiration, policy changes, and delete/recreate. Whole-store
retirement advances every stripe. The epoch is read before opening the
preparation view and checked after acquiring the publication stripe. Equality
proves that the original metadata, including its ID, generation, revision,
policy, ownership and activity time, still describes the prepared bytes.

An intervening mutation causes preparation to restart, at most four times.
An unrelated name sharing the stripe may also force a conservative retry.
Epochs saturate rather than wrap; a saturated stripe refuses publication.
No part of a stale or exhausted repair is committed.

The replacement groups and next revision are encoded before locking. The
stripe covers the epoch check, cache invalidation, and one atomic Pebble
`NoSync` commit. The durability barrier and read-cache invalidation remain
after unlock. `lock_ns` reports the maximum hold across attempts and includes
metadata-cache cleanup. This removes cold reads from the critical section;
commit stalls and scheduler delays still require measurement on a live canary.

Tests retain the previous implementation as an independent publication oracle,
compare complete exported bytes with and without metadata caching, inject a
write between preparation and publication, exercise delete/recreate and the
retry cap, and reproduce cold metadata reads using a delayed filesystem.
`BenchmarkAnchorLock581` reports both mean and maximum lock hold over the exact
581/562 fixture.
