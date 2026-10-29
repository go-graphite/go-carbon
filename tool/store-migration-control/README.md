# Online conversion admission

`python3 main.py --config /private/controller.json --baseline` records one minute
of the running candidate/control services without changing a worker lease.
Run without `--baseline` after selecting the migration binary. It adjusts only
the version-1 `migration-control.json` lease; it never restarts a service or
rewrites metric data. After identifying the expected serving worker, it creates
and checks a small `.go-carbon-incremental-source-root.keep` file in the source
root. This preserves the bound directory inode when empty-directory cleanup runs
after the final metric is converted. Keep this guard and the source directory
while incremental mode remains enabled. Foreign, replaced, symlinked or special
guard files stop admission. The application must allow 64 migration workers.

The configuration contains `version: 1`, `hostname`, `candidate_host` (loopback
`127.0.0.1`), `control_host`, `directory`, `source_directory`, `physical_device`,
`candidate_config`, both `candidate_config_sha256` and `candidate_binary_sha256`,
`limits_file`, `min_free_bytes`, and optional `quiet_local_minutes` pairs.
Directory/path settings are absolute; quiet windows use the host timezone.
The directory must already exist and contain an initial version-1 worker lease.

Atomically replace the limits file with `{"max_workers": 16, "paused": false}`
to change the ceiling without losing conversion progress. `paused: true` or a
zero ceiling stops new admissions. Existing imports finish. The controller
checks end-to-end request p99, cache level/trend, CPU/RSS, active storage
stalls, physical I/O pressure and free space against the running control and the
recorded baseline. Internal disk-wait histograms remain diagnostic: CWhisper
starts its timer after header open, while shared fetch includes catalog/header
work, so their phase p99 values are unsuitable for cross-backend admission. Insufficient request volume prevents acceleration; failed
observations refresh a zero-worker lease. The app falls back to one worker if
the controller stops and its lease expires.
Equal candidate/replica tails are admissible even when a short baseline put the
candidate in a faster histogram bucket. A current increase beyond the allowed
candidate/replica envelope still reduces admission.
Two healthy observations advance admission through 0, 1, 2, 4, 8, 16, 24,
32, 48 and 64 workers, capped by operator limits. The small initial stages
probe conversion load before admitting larger groups. They share all request,
backlog and resource guards; there is no guaranteed worker floor during a
regression, and hot-metric hints use the same admission budget.
The p99 estimate uses linear interpolation inside cumulative duration buckets;
status also records the enclosing lower/upper bucket bounds. An unbounded tail
stops admissions. The baseline is recalculated from its saved raw samples using
the same method, without collecting another baseline or restarting go-carbon.

Quiesce external source writers. Quiet windows must start early enough for
in-flight imports to drain before scheduled source maintenance. An unexpected
writer stops new admissions but cannot undo an overlapping external mutation.
After reclamation, rollback requires a compatible binary or data restore.

Validation: `PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s
tool/store-migration-control -p 'test_*.py'` from the repository root. This
controller does not certify API correctness or final performance parity.

## Reboot-stable volume binding

Linux device-mapper numbers can change across reboot. V6's original source,
archive and source-root guard record that number together with each inode;
an unchanged filesystem can consequently fail startup. `volume_binding.py`
records an XFS filesystem UUID and the source/store/archive directory inodes
while their existing bindings are valid. It never adopts a mismatching legacy
binding without a recorded proof; diagnose that case separately.

```sh
sudo install -d -m 0700 /etc/go-carbon-storage-guard
sudo install -m 0700 volume_binding.py /etc/go-carbon-storage-guard/volume_binding.py
sudo python3 -I /etc/go-carbon-storage-guard/volume_binding.py record \
  --proof /etc/go-carbon-storage-guard/volume-binding.json \
  --source /data/whisper --store /data/shared-exact --archive /data/originals
sudo python3 -I /etc/go-carbon-storage-guard/volume_binding.py inspect \
  --proof /etc/go-carbon-storage-guard/volume-binding.json
```

Omit `--archive` on reclaiming nodes. The helper, proof and every parent must
remain outside application-writable directories. Both pilots use the protected
`/etc` paths above. Add this to the service drop-in:

```ini
[Service]
ExecStartPre=+/usr/bin/python3 -I /etc/go-carbon-storage-guard/volume_binding.py rebind --proof /etc/go-carbon-storage-guard/volume-binding.json
```

The prefix runs the metadata repair as root even when go-carbon uses `User=carbon`.
An ordinary same-device start only validates. Before changing device fields,
the helper requires the same filesystem UUID, paths, all recorded directory
inodes and V6 policy, then obtains Pebble's **fcntl record lock**. `flock` is
independent on Linux and cannot fence a live Pebble engine. It saves the old
binding bytes and atomically syncs each replacement; interrupted updates can
resume. It never rewrites SSTs, WALs or metric data. Replaced directories,
unrecorded archive policies, symlinks, unavailable UUIDs and non-XFS mounts fail.
Existing compatible V6 executables remain usable because marker formats do not
change. The drop-in need not restart a serving process when installed.
