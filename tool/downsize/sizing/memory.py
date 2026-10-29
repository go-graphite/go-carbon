"""Target-shape RAM budgets, deduplicating Go-backed metadata and cgo caches."""
GIB = 2 ** 30
COMPONENTS = ("heap_live_bytes", "application_heap_live_bytes", "non_heap_anon_bytes",
              "memtable_bytes", "active_file_bytes", "slab_bytes", "block_cache_bytes", "metadata_cache_bytes")


def cache_budgets(shape, policy):
    ram = shape["ram_gib"] * GIB
    block = shape.get("block_cache_gib", policy.block_cache_fraction * shape["ram_gib"]) * GIB
    metadata = shape.get("metadata_cache_gib", policy.metadata_cache_fraction * shape["ram_gib"]) * GIB
    if block + metadata >= ram * policy.memory_target:
        raise ValueError(f'{shape["name"]}: cache budgets consume the whole RAM target')
    return block, metadata


def ram_model(hosts, shape, policy, percentile, skew, survivors=None):
    skew *= policy.memory_headroom
    gc_factor = shape.get("go_gc_factor", policy.go_gc_factor)
    legacy = sum(h["metrics"]["working_set_bytes"][percentile] for h in hosts) * skew
    missing = {h["host"]: [f for f in COMPONENTS if h["metrics"].get(f, {}).get(percentile) is None
                           or h["metrics"][f].get("coverage", 0) < policy.coverage_min] for h in hosts}
    missing = {host: fields for host, fields in missing.items() if fields}
    for host in hosts:
        if host["metrics"].get("heap_live_bytes", {}).get(percentile) == 0:
            missing.setdefault(host["host"], []).append("heap_live_bytes is zero")
    block, metadata = cache_budgets(shape, policy)
    available = not missing
    use_components = policy.ram_mode == "components" and available
    rows, variable, peak = [], 0, 0
    if available:
        for host in hosts:
            m = host["metrics"]
            current = {f: m[f][percentile] for f in COMPONENTS}
            current["modeled_variable_bytes"] = gc_factor * current["application_heap_live_bytes"] + sum(
                current[f] for f in ("non_heap_anon_bytes", "memtable_bytes", "active_file_bytes", "slab_bytes"))
            rows.append({"host": host["host"], "percentile": percentile, "value": current["modeled_variable_bytes"], **current})
            variable += current["modeled_variable_bytes"] * skew
            def full_peak(field):
                value = host.get("burst_metrics", {}).get(field, {}).get("peak")
                return m[field]["peak"] if value is None else value
            peak += (gc_factor * full_peak("application_heap_live_bytes") + sum(
                full_peak(f) for f in ("non_heap_anon_bytes", "memtable_bytes", "active_file_bytes", "slab_bytes"))) * skew
    # Metadata arenas are inside the Go heap, so their target retained budget
    # participates in the GC factor. The block cache is outside it on cgo builds.
    fixed = block + metadata * gc_factor
    result = {"mode": "components" if use_components else "legacy-working-set", "components_available": available,
              "missing": missing, "legacy_working_set_bytes": legacy, "component_variable_bytes": variable if available else None,
              "component_peak_variable_bytes": peak if available else None, "fixed_cache_bytes_per_node": fixed,
              "block_cache_budget_bytes": block, "metadata_cache_budget_bytes": metadata,
              "gc_factor": gc_factor, "target_gogc": round((gc_factor - 1) * 100), "skew": skew,
              "datapoints": rows, "note": "Application live excludes Go metadata arenas; RSS residual excludes resident Go heap and cgo cache/memtables",
              "hit_rate_implications": "Shrinking caches can increase read IOPS/CPU/p99; no hit-rate preservation is assumed. Canary the new budgets and GOGC.",
              "observed_hit_ratios": [{"host": h["host"], **{f: h["metrics"].get(f, {}).get(percentile)
                                      for f in ("block_cache_hit_ratio", "metadata_cache_hit_ratio")}} for h in hosts]}
    if survivors:
        result.update(legacy_per_survivor_bytes=legacy / survivors,
                      component_per_survivor_bytes=variable / survivors + fixed if available else None,
                      old_component_per_survivor_cache_bytes=sum(h["metrics"]["block_cache_bytes"][percentile] +
                          h["metrics"]["metadata_cache_bytes"][percentile] for h in hosts) / survivors if available else None)
    return result
