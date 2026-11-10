# Exact point maintenance

The maintenance API supports a single finite-value correction and bounded
missing-only fills for existing continuation-backed metrics. These operations
preserve metric identity, retention policy, other archives, and every point
outside the request. They do not pass through ordinary intake or propagate
rollups.

Inspect `GET /v1/native/capabilities` before preparing a request. The daemon
installs the live cache fence before starting the service; a standalone service
without that fence cannot publish a patch. Dry runs require root read authority;
requests with `apply: true` require root update authority.

## One finite point

`POST /v1/maintenance/finite-point` accepts an exact committed version from
native export and a single archive-zero timestamp. For example, this dry run
plans a change from the IEEE-754 representation of `1` to `2`:

```json
{
  "expected": {"Name": "example.requests", "ID": 1, "Generation": 1, "Revision": 4},
  "archive": 0,
  "step": 60,
  "timestamp": 1800000000,
  "before_bits": "3ff0000000000000",
  "donor_bits": "4000000000000000",
  "plan_sha256": "1111111111111111111111111111111111111111111111111111111111111111",
  "nonce": "22222222222222222222222222222222",
  "apply": false
}
```

Use the actual export version, timestamp, preimage bits, immutable plan digest,
and fresh nonce for a real correction. Set `apply` only after reconciling the dry
run with that plan. Both values must be finite and different. Publication fences
the committed version and live cache, then records the consumed plan durably.

## Missing-only fills

`POST /v1/maintenance/missing-only` accepts one existing metric and 1–128 unique
points ordered by archive and timestamp. Each point names its archive, step,
count, timestamp, `before_bits`, and `donor_bits`. An empty preimage means an
absent timestamp; a populated preimage must be the exact NaN payload. Existing
non-NaN values cannot be overwritten by this operation.

The request binds its expected version, plan, batch, parent proof, projection,
source binding, and nonce. `MissingPointHash` defines the canonical batch digest.
The response reports requested, preimage, and applied digests. Donor values may
include signed infinity when `AtomicMissingOnlyNonNaN` is advertised; their bits
are preserved exactly. NaN donor values remain invalid.

Both operations reject concurrent cache input, changed versions, incompatible
ownership, whole-store copy, and storage pressure. They publish at most once per
receipt. If an acknowledgement is lost, reconcile the stored points and receipt
before preparing any new request; never blindly replay a consumed plan.

Archive-anchor inspection also reports identical physical duplicates separately
from conflicting aliases. Anchor repair retains their complete bitwise multiset;
point maintenance refuses ambiguous duplicate layouts.
