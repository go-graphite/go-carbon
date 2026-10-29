The MIT-licensed buckytools hashing algorithms are retained in hashing.go,
fnv1a.go and jump.go; see LICENSE for their original copyright and attribution.
fast.go compiles the ring into a 16-bit lookup table, shares key hashes between
layouts and uses a bounded stack copy for short MD5 inputs. The original
functions provide a differential oracle for this accelerated implementation.
Jump replication above one is rejected because the original implementation's
scratch-ring copy can panic; use one physical copy per ring for jump hashing.
