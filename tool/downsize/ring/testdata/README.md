The committed vectors cover FNV-1a 64-bit hashing and jump consistent hashing
with 1, 2, 3, 6, 8, 64 and 666 buckets. Tests compare the planner, buckytools
algorithms and the hash ring advertised by Buckyd. Names and node inventories
are synthetic examples. The committed sample plan has an independently checked
manifest identity and stream checksums. generate-inventory.go creates larger
sorted inventories for local benchmarks without contacting a store.
