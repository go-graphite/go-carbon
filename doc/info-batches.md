# Partial metadata batches

The HTTP v3 `/info/?format=carbonapi_v3_pb&allowPartial=1` endpoint returns
metadata for the requested names that exist, in request order. Missing names
are omitted. An entirely absent batch returns HTTP 200 with an empty
`MultiMetricsInfoResponse`. Retentions and other metadata fields are identical
to individual requests.

Without the explicit option, and for other response formats, the existing
all-or-nothing behavior remains: a missing name returns HTTP 404.

`storectl sync` requests partial metadata for its verification and mutation
frontier checks. This matters when checking a union catalog against holders
that own only a fraction of its names: an absent 500-name batch takes one
request instead of 999 recursive requests. All-holder schema checks remain in
place before and after comparison. Older servers continue to use the existing
404 bisection; upgrade the stores and the client to gain the batching benefit.
