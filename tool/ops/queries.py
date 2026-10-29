"""PromQL reports. Values retain their native units unless the name says otherwise."""
import json
import re


def label_name(value):
    if not re.fullmatch(r'[a-zA-Z_][a-zA-Z0-9_]*', value):
        raise ValueError('invalid label name')
    return value


def selector(text, host_label, hosts):
    if any(c in text for c in '{}\n\r'):
        raise ValueError('selector must contain label matchers without braces')
    if hosts:
        matcher = label_name(host_label) + '=~' + json.dumps('|'.join(re.escape(h) for h in hosts))
        text = ','.join(x for x in (text, matcher) if x)
    return text


def queries(report, select='', host='instance', window='10m', mountpoint='/var/lib/carbon', backend='pebble'):
    label_name(host)
    if not re.fullmatch(r'[1-9][0-9]*(ms|s|m|h|d|w)', window):
        raise ValueError('window must be a positive PromQL duration')

    def metric(name, extra=''):
        return name + '{' + ','.join(x for x in (select, extra) if x) + '}'

    def gauge(name):
        return 'max by (%s) (%s)' % (host, metric(name))

    def rate(name, extra=''):
        return 'sum by (%s) (rate(%s[%s]))' % (host, metric(name, extra), window)

    def increase(name):
        return 'sum by (%s) (increase(%s[%s]))' % (host, metric(name), window)

    def p99(name):
        return 'histogram_quantile(0.99, sum by (le,%s) (rate(%s[%s])))' % (host, metric(name + '_bucket'), window)

    health = {'up': gauge('up'), 'cpu_cores': rate('process_cpu_seconds_total'),
              'rss_bytes': gauge('process_resident_memory_bytes'),
              'read_p99_seconds': p99('http_request_duration_seconds_exp'),
              'requests_per_second': rate('http_request_duration_seconds_exp_count'),
              'write_errors': increase('persister_pebble_write_failures_total'),
              'migration_in_flight': gauge('store_migration_in_flight'),
              'migration_failed': gauge('store_migration_failed')}
    if report in ('health', 'parity'):
        return health
    if report == 'migration':
        return {**{name: gauge('store_migration_' + name) for name in
                   ('running', 'in_flight', 'workers', 'failed', 'reclaim_queued', 'waiting_for_disk_space')},
                'converted_per_second': rate('store_migration_converted_total'),
                'reclaimed_bytes_per_second': rate('store_migration_reclaimed_bytes_total'),
                'reclaim_completed_per_second': rate('store_migration_reclaim_completed_total'),
                'skipped': gauge('store_migration_skipped_total')}
    if report == 'cost':
        if backend not in ('pebble', 'whisper'):
            raise ValueError('backend must be pebble or whisper')
        points = rate('persister_' + backend + '_points_total')
        updates = rate('persister_' + backend + '_updates_total')
        return {'points_per_second': points, 'updates_per_second': updates,
                'points_per_update': '(%s) / (%s)' % (points, updates),
                'process_cpu_seconds_per_point': '(%s) / (%s)' % (rate('process_cpu_seconds_total'), points),
                **{stage + '_seconds_per_point': '(%s) / (%s)' %
                   (rate('persister_stage_seconds_total', 'stage=' + json.dumps(stage)), points)
                   for stage in ('total', 'route', 'lock_wait', 'load', 'tail_read', 'decode', 'encode', 'ooo_merge', 'commit', 'original_write')}}
    if report == 'gc':
        total = rate('go_cpu_classes_total_cpu_seconds_total')
        idle = rate('go_cpu_classes_idle_cpu_seconds_total')
        return {'gc_busy_cpu_ratio': '(%s) / ((%s) - (%s))' % (rate('go_cpu_classes_gc_total_cpu_seconds_total'), total, idle),
                'allocated_bytes_per_second': rate('go_gc_heap_allocs_bytes_total'),
                **{name: gauge(name) for name in
                   ('go_gc_heap_live_bytes', 'go_gc_heap_goal_bytes', 'go_gc_scan_heap_bytes',
                    'process_resident_memory_bytes', 'store_metadata_cache_bytes',
                    'store_metadata_cache_arena_bytes', 'pebble_block_cache_bytes', 'cache_estimated_bytes')}}
    if report == 'input-log':
        return {'unlogged_points': increase('input_log_unlogged_points_total'),
                'logged_points_per_second': rate('input_log_points_written_total'),
                'write_errors': increase('input_log_write_errors_total'),
                'bytes_per_second': rate('input_log_bytes_written_total'),
                'sync_p99_seconds': p99('input_log_sync_seconds_exp'),
                'oldest_unpersisted_seconds': gauge('input_log_oldest_unpersisted_age_seconds'),
                **{stage + '_seconds_per_cycle': '(%s) / (%s)' %
                   (rate('input_log_stage_seconds_total', 'stage=' + json.dumps(stage)), rate('input_log_cycles_total'))
                   for stage in ('collect', 'encode', 'compress', 'sync', 'relog', 'roll', 'write')}}
    if report == 'io':
        return {'compaction_debt_bytes': gauge('pebble_compaction_debt_bytes'),
                'l0_sublevels': gauge('pebble_l0_sublevels'),
                'stall_seconds_per_second': rate('pebble_write_stall_seconds_total'),
                **{name: rate(name) for name in
                   ('pebble_compaction_write_bytes_total', 'pebble_compaction_read_bytes_total',
                    'pebble_flush_bytes_total', 'pebble_ingest_bytes_total', 'pebble_ingest_entries_total',
                    'pebble_ingest_tables_total')}}
    if report == 'capacity':
        fs = metric('node_filesystem_size_bytes', 'mountpoint=' + json.dumps(mountpoint))
        free = metric('node_filesystem_avail_bytes', 'mountpoint=' + json.dumps(mountpoint))
        return {'cpu_cores': rate('process_cpu_seconds_total'),
                'memory_bytes': gauge('process_resident_memory_bytes'),
                'disk_bytes': 'max by (%s) (%s - %s)' % (host, fs, free)}
    raise ValueError('unknown report ' + report)
