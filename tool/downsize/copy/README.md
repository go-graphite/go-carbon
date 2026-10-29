# Legacy checkpoint entrypoint

This binary delegates to `resize snapshot` in the shared Go engine. Use the
[resize runbook](../README.md) for replacements and mixed topology changes.
`control.py preflight` supplies the target capacity/configuration report.
`benchmark.py` and `read_block.py` remain local benchmark/legacy diagnostic helpers.
The live resource governor and preparation path are shared Go code.
