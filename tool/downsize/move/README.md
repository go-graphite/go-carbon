# Legacy move entrypoint

This binary delegates to the shared resize engine. Use the [resize runbook](../README.md)
for topology changes, replacements, mixed plans, host guards and resume.
`host.py` remains the host telemetry/read-isolation helper; `lab/` is the local
fixture/ingestion benchmark. [Measurements](BENCHMARKS.md).
