# Legacy checkpoint entrypoint

This binary delegates to `resize snapshot` in the shared Go engine. Use the
[resize runbook](../README.md) for replacements and mixed topology changes.
`control.py preflight` supplies the target capacity/configuration report.
`benchmark.py` and `read_block.py` remain local benchmark/legacy diagnostic helpers.
The live resource governor and preparation path are shared Go code.

For a copy-only canary that leaves foreign stale names on a serving store,
`read_block.py --retain-block --state /var/tmp/canary/read-block.json --apply`
uses the usual source/peer/site/cluster/probes arguments and validates peer
history before installing IPv4/IPv6 rejection rules. This explicit operator hold
has no expiry timer or process-exit cleanup. Keep the state and script on the
host. Release with `python3 read_block.py --release-state <state>` only after
copied names have been removed or kept current and read correctness verified.
These are live firewall rules; a host reboot or external firewall replacement
requires re-establishing isolation before reads resume. The default lease mode
is unchanged. Do not combine this hold with a controller's `--block-reads` lease.
