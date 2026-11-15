# TCP intake pressure

On Linux, go-carbon samples TCP intake queues and kernel TCP counters every 10s.
Prometheus scrapes and internal Graphite statistics read the same cached snapshot;
no work is added to connection handling. Prometheus uses the configured labels
and adds `receiver` for listener gauges. Internal stats use the `tcp` module,
with a receiver-name prefix for listener gauges. Other platforms do nothing.

| Metric | Meaning |
| --- | --- |
| `receiver_tcp_accept_queue_length` | Connections waiting for accept |
| `receiver_tcp_accept_queue_limit` | Accept backlog limit |
| `receiver_tcp_receive_queue_bytes` | Total unread bytes in live connections |
| `receiver_tcp_connections` | Live connections, excluding pending handshakes and TIME_WAIT |
| `host_tcp_listen_overflows_total` | TcpExt ListenOverflows |
| `host_tcp_listen_drops_total` | TcpExt ListenDrops |
| `host_tcp_backlog_drops_total` | TcpExt TCPBacklogDrop |
| `host_tcp_receive_queue_drops_total` | TcpExt TCPRcvQDrop |
| `host_tcp_prune_called_total` | TcpExt PruneCalled |
| `host_tcp_receive_pruned_total` | TcpExt RcvPruned |
| `host_tcp_out_of_order_pruned_total` | TcpExt OfoPruned |
| `host_tcp_zero_window_drops_total` | TcpExt TCPZeroWindowDrop |
| `host_tcp_to_zero_window_advertisements_total` | TcpExt TCPToZeroWindowAdv |
| `host_tcp_want_zero_window_advertisements_total` | TcpExt TCPWantZeroWindowAdv |
| `host_tcp_input_errors_total` | Tcp InErrs |
| `host_tcp_input_checksum_errors_total` | Tcp InCsumErrors |

Listener gauges use `NETLINK_INET_DIAG`, with a kernel source-port filter, for
each bound TCP receiver address (including plaintext, pickle and custom HTTP
receivers). `/proc/net/tcp` does not expose the accept queue limit. Queues cover
the address/port, so a shared-port listener can include other processes' sockets.
The sampler starts after receiver setup, including inherited/startup-capture
listeners, and stops during intake shutdown. It needs no additional privileges.

Host counters read `/proc/net/netstat` and `/proc/net/snmp`. They cover the current
network namespace, including other applications, and reset with that namespace,
not when go-carbon restarts. Use `rate()` for Prometheus; internal Graphite stats
also publish the raw cumulative value. Do not add listen drops and overflows to
estimate lost points: the same event can affect both counters, TCP retries can
recover packets, and application-level relay drops are a separate measurement.

Missing kernel counters and failed listener samples are omitted. Check
`host_tcp_sample_success` and `receiver_tcp_sample_success` before interpreting
missing data. `receiver_tcp_sample_duration_seconds` measures sampling cost;
cost depends on the socket population and kernel. To measure it locally on Linux:

```sh
go test ./receiver/socketstats -run Test -bench BenchmarkSample -benchmem
```

For example, accept saturation can be graphed with:

```promql
receiver_tcp_accept_queue_length / receiver_tcp_accept_queue_limit
```

An increasing receive queue together with zero-window advertisements indicates
that userspace is not keeping up with incoming data. Counters cannot reconstruct
queue occupancy before the sampler was deployed.
