These are three captured transfer images with their metric names replaced by
`example.duration_ms`; encoded archive keys and values are unchanged.

The hourly retention is 3600:216. Both sources hold 54.833333333333336 at
1794042000. The target instead holds zero at 4026531600 in the same slot
(2871 full rings later). All three also retain an unrelated daily future zero
at 4026499200. The regression fixes the clock at 1794759182 so the hourly
source point is still within retention.

Policy: a point in `(now - step*count, now]` wins over a future timestamp
sharing its ring slot. Otherwise the newest epoch wins, with finite target
values winning equal timestamps during combine. This applies to ordinary
circular writes, re-anchoring, transfer and native combine. Window copies use
the same rule and cannot let a future timestamp advance their expiration
cutoff beyond the current clock. No blanket future-time filter is applied:
conversion and unaliased future history retain their exact encoded points.

The tests check both independent donors, the transfer/native/window paths,
read-after-import, subsequent writes and bit-exact preservation of retained
points. They also require the unrelated daily future point to survive.
