`stream-v1.hex` is a version 1 uncompressed connection, encoded as lowercase hex
with one terminating newline. It includes the preamble and four frames:

1. Define `example.requests` (id 0) and `example.latency` (id 1). Emit requests
   `(1, 1700000000)`, requests `(2, 1700000001)`, latency `(1.25, 1700000000)`.
2. Emit latency `(1.5, 1700000001)`.
3. Reset the dictionary.
4. Define requests as id 0 and emit `(3, 1700000002)`.

Pairs are `(value, timestamp)`. The first frame demonstrates stable id sorting
of the input requests, latency, requests. A sender in another language can check
its exact wire output against this fixture. `TestGolden` fixes the input order.
