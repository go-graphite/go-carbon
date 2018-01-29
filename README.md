Table of Contents
=================

* [Features](#features)
* [Performance](#performance)
* [Installation](#installation)
* [Configuration](#configuration)
  * [OS tuning](#os-tuning)
* [Reported stats](#reported-stats)
* [Changelog](#changelog)
  * [master](#master)


go-carbon [![Build Status](https://travis-ci.org/lomik/go-carbon.svg?branch=master)](https://travis-ci.org/lomik/go-carbon)
============

Golang implementation of Graphite/Carbon server with classic architecture: Agent -> Cache -> Persister

![Architecture](doc/design.png)

## Features
- Receive metrics from TCP and UDP ([plaintext protocol](http://graphite.readthedocs.org/en/latest/feeding-carbon.html#the-plaintext-protocol))
- Receive metrics with [Pickle protocol](http://graphite.readthedocs.org/en/latest/feeding-carbon.html#the-pickle-protocol) (TCP only)
- Receive metrics from HTTP
- Receive metrics from Apache Kafka
- [storage-schemas.conf](http://graphite.readthedocs.org/en/latest/config-carbon.html#storage-schemas-conf)
- [storage-aggregation.conf](http://graphite.readthedocs.org/en/latest/config-carbon.html#storage-aggregation-conf)
- Carbonlink (requests to cache from graphite-web)
- Carbonlink-like GRPC api
- Logging with rotation support (reopen log if it moves)
- Many persister workers (using many cpu cores)
- Run as daemon
- Optional dump/restore restart on `USR2` signal (config `dump` section): stop persister, start write new data to file, dump cache to file, stop all (and restore from files after next start)
- Reload some config options without restart (HUP signal):
  - `whisper` section of main config, `storage-schemas.conf` and `storage-aggregation.conf`
  - `graph-prefix`, `metric-interval`, `metric-endpoint`, `max-cpu` from `common` section
  - `dump` section

## Performance

Faster than default carbon. In all conditions :) How much faster depends on server hardware, storage-schemas, etc.

The result of replacing "carbon" to "go-carbon" on a server with a load up to 900 thousand metric per minute:

![Success story](doc/success1.png)

There were some efforts to find out maximum possible performance of go-carbon on a hardware (2xE5-2620v3, 128GB RAM, local SSDs).

The result of that effort (in points per second):

![Performance](doc/performance.png)

Stable performance was around 950k points per second with short-term peak performance of 1.2M points/sec.

## Installation
Use binary packages from [releases page](https://github.com/lomik/go-carbon/releases) or build manually (requires golang 1.8+):
```
# build binary
git clone https://github.com/lomik/go-carbon.git
cd go-carbon
make submodules
make
```

## Configuration
```
$ go-carbon --help
Usage of go-carbon:
  -check-config=false: Check config and exit
  -config="": Filename of config
  -config-print-default=false: Print default config
  -daemon=false: Run in background
  -pidfile="": Pidfile path (only for daemon)
  -version=false: Print version
```

```toml
[common]
# Run as user. Works only in daemon mode
user = "carbon"
# Prefix for store all internal go-carbon graphs. Supported macroses: {host}
graph-prefix = "carbon.agents.{host}"
# Endpoint for store internal carbon metrics. Valid values: "" or "local", "tcp://host:port", "udp://host:port"
metric-endpoint = "local"
# Interval of storing internal metrics. Like CARBON_METRIC_INTERVAL
metric-interval = "1m0s"
# Increase for configuration with multi persister workers
max-cpu = 4

[whisper]
data-dir = "/var/lib/graphite/whisper"
# http://graphite.readthedocs.org/en/latest/config-carbon.html#storage-schemas-conf. Required
schemas-file = "/etc/go-carbon/storage-schemas.conf"
# http://graphite.readthedocs.org/en/latest/config-carbon.html#storage-aggregation-conf. Optional
aggregation-file = "/etc/go-carbon/storage-aggregation.conf"
# Worker threads count. Metrics sharded by "crc32(metricName) % workers"
workers = 8
# Limits the number of whisper update_many() calls per second. 0 - no limit
max-updates-per-second = 0
# Sparse file creation
sparse-create = false
# use flock on every file call (ensures consistency if there are concurrent read/writes to the same file)
flock = false
enabled = true

[cache]
# Limit of in-memory stored points (not metrics)
max-size = 1000000
# Capacity of queue between receivers and cache
# Strategy to persist metrics. Values: "max","sorted","noop"
#   "max" - write metrics with most unwritten datapoints first
#   "sorted" - sort by timestamp of first unwritten datapoint.
#   "noop" - pick metrics to write in unspecified order,
#            requires least CPU and improves cache responsiveness
write-strategy = "max"

[udp]
listen = ":2003"
enabled = true
# Enable optional logging of incomplete messages (chunked by max UDP packet size)
log-incomplete = false
# Optional internal queue between receiver and cache
buffer-size = 0

[tcp]
listen = ":2003"
enabled = true
# Optional internal queue between receiver and cache
buffer-size = 0

[pickle]
listen = ":2004"
# Limit message size for prevent memory overflow
max-message-size = 67108864
enabled = true
# Optional internal queue between receiver and cache
buffer-size = 0

# You can define unlimited count of additional receivers
# Common definition scheme:
# [receiver.<any receiver name>]
# protocol = "<any supported protocol>"
# <protocol specific options>
#
# All available protocols:
#
# [receiver.udp2]
# protocol = "udp"
# listen = ":2003"
# # Enable optional logging of incomplete messages (chunked by max UDP packet size)
# log-incomplete = false
#
# [receiver.tcp2]
# protocol = "tcp"
# listen = ":2003"
#
# [receiver.pickle2]
# protocol = "pickle"
# listen = ":2004"
# # Limit message size for prevent memory overflow
# max-message-size = 67108864
#
# [receiver.protobuf]
# protocol = "protobuf"
# # Same framing protocol as pickle, but message encoded in protobuf format
# # See https://github.com/lomik/go-carbon/blob/master/helper/carbonpb/carbon.proto
# listen = ":2005"
# # Limit message size for prevent memory overflow
# max-message-size = 67108864
#
# [receiver.http]
# protocol = "http"
# # This receiver receives data from POST requests body.
# # Data can be encoded in plain text format (default),
# # protobuf (with Content-Type: application/protobuf header) or
# # pickle (with Content-Type: application/python-pickle header).
# listen = ":2007"
# max-message-size = 67108864
#
# [receiver.kafka]
# protocol = "kafka
# # This receiver receives data from kafka
# # You can use Partitions and Topics to do sharding
# # State is saved in local file to avoid problems with multiple consumers
#
# # Encoding of messages
# # Available options: "plain" (default), "protobuf", "pickle"
# #   Please note that for "plain" you must pass metrics with leading "\n".
# #   e.x.
# #    echo "test.metric $(date +%s) $(date +%s)" | kafkacat -D $'\0' -z snappy -T -b localhost:9092 -t graphite
# parse-protocol = "protobuf"
# # Kafka connection parameters
# brokers = [ "host1:9092", "host2:9092" ]
# topic = "graphite"
# partition = 0
#
# # Specify how often receiver will try to connect to kafka in case of network problems
# reconnect-interval = "5m"
# # How often receiver will ask Kafka for new data (in case there was no messages available to read)
# fetch-interval = "200ms"
#
# # Path to saved kafka state. Used for restarts
# state-file = "/var/lib/graphite/kafka.state"
# # Initial offset, if there is no saved state. Can be relative time or "newest" or "oldest".
# # In case offset is unavailable (in future, etc) fallback is "oldest"
# initial-offset = "-30m"
#
# # Specify kafka feature level (default: 0.11.0.0).
# # Please note that some features (consuming lz4 compressed streams) requires kafka >0.11
# # You must specify version in full. E.x. '0.11.0.0' - ok, but '0.11' is not.
# # Supported version (as of 22 Jan 2018):
# #   0.8.2.0
# #   0.8.2.1
# #   0.8.2.2
# #   0.9.0.0
# #   0.9.0.1
# #   0.10.0.0
# #   0.10.0.1
# #   0.10.1.0
# #   0.10.2.0
# #   0.11.0.0
# #   1.0.0
# kafka-version = "0.11.0.0"
#
# [receiver.pubsub]
# # This receiver receives data from Google PubSub
# # - Authentication is managed through APPLICATION_DEFAULT_CREDENTIALS:
# #   - https://cloud.google.com/docs/authentication/production#providing_credentials_to_your_application
# # - Currently the subscription must exist before running go-carbon.
# # - The "receiver_*" settings are optional and directly map to the google pubsub
# #   libraries ReceiveSettings (https://godoc.org/cloud.google.com/go/pubsub#ReceiveSettings)
# #   - How to think about the "receiver_*" settings: In an attempt to maximize throughput the
# #     pubsub library will spawn 'receiver_go_routines' to fetch messages from the server.
# #     These goroutines simply buffer them into memory until 'receiver_max_messages' or 'receiver_max_bytes'
# #     have been read. This does not affect the actual handling of these messages which are processed by other goroutines.
# protocol = "pubsub"
# project = "project-name"
# subscription = "subscription-name"
# receiver_go_routines = 4
# receiver_max_messages = 1000
# receiver_max_bytes = 500000000 # default 500MB


[carbonlink]
listen = "127.0.0.1:7002"
enabled = true
# Close inactive connections after "read-timeout"
read-timeout = "30s"

# grpc api
# protocol: https://github.com/lomik/go-carbon/blob/master/helper/carbonpb/carbon.proto
# samples: https://github.com/lomik/go-carbon/tree/master/api/sample
[grpc]
listen = "127.0.0.1:7003"
enabled = true

# http://graphite.readthedocs.io/en/latest/tags.html
[tags]
enabled = false
# TagDB url. It should support /tags/tagMultiSeries endpoint
tagdb-url = "http://127.0.0.1:8000"
tagdb-chunk-size = 32
# Directory for send queue (based on leveldb)
local-dir = "/var/lib/graphite/tagging/"
# POST timeout
tagdb-timeout = "1s"

[carbonserver]
# Please NOTE: carbonserver is not intended to fully replace graphite-web
# It acts as a "REMOTE_STORAGE" for graphite-web or carbonzipper/carbonapi
listen = "127.0.0.1:8080"
# Carbonserver support is still experimental and may contain bugs
# Or be incompatible with github.com/grobian/carbonserver
enabled = false
# Buckets to track response times
buckets = 10
# carbonserver-specific metrics will be sent as counters
# For compatibility with grobian/carbonserver
metrics-as-counters = false
# Read and Write timeouts for HTTP server
read-timeout = "60s"
write-timeout = "60s"
# Enable /render cache, it will cache the result for 1 minute
query-cache-enabled = true
# 0 for unlimited
query-cache-size-mb = 0
# Enable /metrics/find cache, it will cache the result for 5 minutes
find-cache-enabled = true
# Control trigram index
#  This index is used to speed-up /find requests
#  However, it will lead to increased memory consumption
#  Estimated memory consumption is approx. 500 bytes per each metric on disk
#  Another drawback is that it will recreate index every scan-frequency interval
#  All new/deleted metrics will still be searchable until index is recreated
trigram-index = true
# carbonserver keeps track of all available whisper files
# in memory. This determines how often it will check FS
# for new or deleted metrics.
scan-frequency = "5m0s"
# Maximum amount of globs in a single metric in index
# This value is used to speed-up /find requests with
# a lot of globs, but will lead to increased memory consumption
max-globs = 100
# Fail if amount of globs more than max-globs
fail-on-max-globs = false
# graphite-web-10-mode
# Use Graphite-web 1.0 native structs for pickle response
# This mode will break compatibility with graphite-web 0.9.x
# If false, carbonserver won't send graphite-web 1.0 specific structs
# That might degrade performance of the cluster
# But will be compatible with both graphite-web 1.0 and 0.9.x
graphite-web-10-strict-mode = true
# Allows to keep track for "last time readed" between restarts, leave empty to disable
internal-stats-dir = ""
# Calculate /render request time percentiles for the bucket, '95' means calculate 95th Percentile. To disable this feature, leave the list blank
stats-percentiles = [99, 98, 95, 75, 50]

[dump]
# Enable dump/restore function on USR2 signal
enabled = false
# Directory for store dump data. Should be writeable for carbon
path = "/var/lib/graphite/dump/"
# Restore speed. 0 - unlimited
restore-per-second = 0

[pprof]
listen = "localhost:7007"
enabled = false

# Default logger
[[logging]]
# logger name
# available loggers:
# * "" - default logger for all messages without configured special logger
# @TODO
logger = ""
# Log output: filename, "stderr", "stdout", "none", "" (same as "stderr")
file = "/var/log/go-carbon/go-carbon.log"
# Log level: "debug", "info", "warn", "error", "dpanic", "panic", and "fatal"
level = "info"
# Log format: "json", "console", "mixed"
encoding = "mixed"
# Log time format: "millis", "nanos", "epoch", "iso8601"
encoding-time = "iso8601"
# Log duration format: "seconds", "nanos", "string"
encoding-duration = "seconds"

# You can define multiply loggers:

# Copy errors to stderr for systemd
# [[logging]]
# logger = ""
# file = "stderr"
# level = "error"
# encoding = "mixed"
# encoding-time = "iso8601"
# encoding-duration = "seconds"

```

### OS tuning
It is crucial for performance to ensure that your OS tuned so that go-carbon
is never blocked on writes, usually that involves adjusting following sysctl
params on Linux systems:

```bash
# percentage of your RAM which can be left unwritten to disk. MUST be much more than
# your write rate, which is usually one FS block size (4KB) per metric.
sysctl -w vm.dirty_ratio = 80

# percentage of yout RAM when background writer have to kick in and
# start writes to disk. Make it way above the value you see in `/proc/meminfo|grep Dirty`
# so that it doesn't interefere with dirty_expire_centisecs explained below
sysctl -w vm.dirty_background_ratio = 50

# allow page to be left dirty no longer than 10 mins
# if unwritten page stays longer than time set here,
# kernel starts writing it out
sysctl -w vm.dirty_expire_centisecs = $(( 10*60*100 ))
```

Net effect of these 3 params is that with `dirty_*_ratio` params set high
enough multiple updates to a metric don't trigger disk activity. Multiple datapoint
writes are coalesced into single disk write which kernel then writes to disk
in a background.

With settings above applied, best write-strategy to use is "noop"

## Reported stats

| metric | description |
| --- | --- |
| cache.maxSize | |
| cache.metrics | |
| cache.size | |
| cache.queueWriteoutTime | Time in seconds to make a full cycle writing all metrics |
| carbonserver.cache\_partial\_hit | Requests that was partially served from cache |
| carbonserver.cache\_miss | Total cache misses |
| carbonserver.cache\_only\_hit | Requests fully served from the cache |
| carbonserver.cache\_wait\_time\_overhead\_ns | Time spent getting copy of the cache |
| carbonserver.cache\_wait\_time\_ns | Time spent waiting for cache, including overhead |
| carbonserver.cache\_requests | Total metrics we've tried to fetch from cache |
| carbonserver.disk\_wait\_time\_ns | Time spent reading data from disk |
| carbonserver.disk\_requests | Amount of metrics we've tried to fetch from disk |
| carbonserver.points\_returned | Datapoints returned by carbonserver |
| carbonserver.metrics\_returned | Metrics returned by carbonserver |
| persister.maxUpdatesPerSecond | |
| persister.workers | |
| runtime.GOMAXPROCS | |
| runtime.NumGoroutine | |


## Changelog
##### master
* [Tags](http://graphite.readthedocs.io/en/latest/tags.html) support was added (only with [graphite-web](https://github.com/graphite-project/graphite-web))
* carbonserver: support multiple targets in /render queries (graphite-web 1.1.x compatibility)
* flock support for persister and carbonserver
* `cache.max-size` and `cache.write-strategy` can be changed without restart (HUP signal)
* Google PubSub protocol was added. It receives data from PubSub Subscriptions and can decode protobuf, plain, or pickle messages.
  * The default format is plain. Specify protobuf or pickle by adding an attribute named 'content-type' to the PubSub messsages:
    * application/protobuf
    * application/python-pickle
  * Sample configuration:
```
[receiver.pubsub]
protocol = "pubsub"
project = "project-name"
subscription = "subscription-name"
# receiver_go_routines = 4
# receiver_max_messages = 1000
# receiver_max_bytes = 500000000 # default 500MB
```

##### version 0.11.0
* GRPC api for query cache was added
* Added support for an unlimited number of receivers
* Protobuf protocol was added. Sample configuration:
```
[receiver.protobuf]
protocol = "protobuf"
listen = ":2005"
```
* HTTP protocol was added. It receives data from POST requests body. Data can be encoded in plain, pickle (Content-Type: application/python-pickle) and protobuf (Content-Type: application/protobuf) formats. Sample configuration:
```
[receiver.http]
protocol = "http"
listen = ":2006"
```

* Kafka protocol was added. It receives data from Kafka and can decode protobuf, plain or pickle messages. You need manually specify message format in the config file. Sample configuration:
```
[receiver.kafka]
protocol = "kafka"
parse-protocol = "protobuf" # can be also "plain" or "pickle"
brokers = [ "localhost:9092" ]
topic = "graphite"
partition = 0
state-file = "/var/lib/graphite/kafka.state"
initial-offset = "-30m" # In case of absent or damaged state file fetch last 30 mins of messages
```

##### version 0.10.0
Breaking changes:

* common: logfile and log-level in common config section are deprecated
* changed config defaults:
  * user changed to `carbon`
  * whisper directory changed to `/var/lib/graphite/whisper/`
  * schemas config changed to `/etc/go-carbon/storage-schemas.conf`
* rpm:
  * binary moved to `/usr/bin/go-carbon`
  * configs moved to `/etc/go-carbon/`
* deb:
  * binary moved to `/usr/bin/go-carbon`

Other changes:

* common: Requires Go 1.8 or newer
* common: Logging refactored. Format changed to structured JSON. Added support of multiple logging handlers with separate output, level and encoding
* dump/restore: New dump format. Added `go-carbon -cat filename` command for printing dump to console. New version of go-carbon can read old dump
* dump/restore: [fix] go-carbon can not stop after dump (with enabled dump and carbonserver)
* carbonserver: [feature] IdleTimeout is now configurable in carbonserver part
* carbonserver: [feature] support /render query cache (query-cache-\* options in config file)
* carbonserver: [feature] support /metrics/find cache (find-cache-\* option in config file)
* carbonserver: [feature] support /metrics/details handler, that returns information about metrics (require enabled trigram-index)
* carbonserver: [feature] Add config option to disable trigram index (before that to disable index you should set scan-interval to 0)
* carbonserver: [fix] fix #146 (metrics_known was broken if metrics were not sent as counters)

##### version 0.9.1
* Always stop on USR2 signal (previously did not stop with disabled dump/restore) #135

##### version 0.9.0
* Completely new internal architecture
* Removed flush to whisper and stop on `USR2` signal. Use dump/restore instead
* Removed global queue (channel) between receivers and cache, added optional per-receiver queues
* Built-in [carbonserver](https://github.com/grobian/carbonserver) (thanks [Vladimir Smirnov](https://github.com/Civil))
* Added runtime tunables to internal metrics #70

##### version 0.8.1
* Bug fix: The synchronous config reload (HUP signal) and launch of the internal collecting statistics procedure (every "metric-interval") could cause deadlock (thanks [Maxim Ivanov](https://github.com/redbaron))

##### version 0.8.0
* Fully refactored and optimizer cache module (core of go-carbon) (thanks [Maxim Ivanov](https://github.com/redbaron))
* Added `noop` cache.write-strategy (thanks [Maxim Ivanov](https://github.com/redbaron))
* New optional dump/restore functional for minimize data loss on restart
* Refactored internal stat mechanics. `common.graph-prefix` and `common.metric-interval` now can be changed without restart (on HUP signal)
* Customizable internal metrics endpoint. `common.metric-endpoint` param. Valid values: "local" and "" (write directly to whisper), "tcp://host:port", "udp://host:port"

##### version 0.7.3
* Added `cache.write-strategy` option (values "max" or "sorted") (thanks [Alexander Akulov](https://github.com/AlexAkulov))
* `commitedPoints` metric renamed to `committedPoints`

##### version 0.7.2
* Added sparse file creation (`whisper.sparse-create` config option)
* Enable reload in init script (thanks [Jose Riguera](https://github.com/jriguera))
* Clean up schemas parser code (thanks [Dieter Plaetinck](https://github.com/Dieterbe))
* Better go-whisper error handling (thanks [Hiroaki Nakamura](https://github.com/hnakamur))
* Don't try to create whisper file if exists with bad permissions #21

##### version 0.7.1
* Fixed problem: Points in queue (channel) between cache and persister subsystems was invisible for carbonlink

##### version 0.7
* Grace stop on `USR2` signal: close all socket listeners, flush cache to disk and stop carbon
* Reload persister config (`whisper` section of main config, `storage-schemas.conf` and `storage-aggregation.conf`) on `HUP` signal 
* Fix bug: Cache may start save points only after first checkpoint
* Decimal numbers in log files instead of hexademical #22
* Fix bug: NaN values being saved in Whisper datafiles #17 (thanks [Andrew Hayworth](https://github.com/ahayworth))
* Fix bug: may crash on bad pickle message with big message size in header #30. Added option `pickle.max-message-size` with 64 MB default value
* Improved throttling (max-updates-per-second) performance #32

##### version 0.6
* `metric-interval` option 

##### version 0.5.5
* Cache module optimization

##### version 0.5.4
* Fix RPM init script

##### version 0.5.3
* Improved validation of bad wsp files
* RPM init script checks config before restart
* Debug logging of bad pickle messages

##### version 0.5.2
* Fix bug in go-whisper library: UpdateMany saves first point if many points has identical timestamp

##### version 0.5.1
* Reduced error level of "bad messages" in tcp and pickle receivers. Now `info`
* Configurable logging level. `log-level` option
* Fix `wrong carbonlink request` error in log

##### version 0.5.0
* `-check-config` validates schemas and aggregation configs
* Fix broken internal metrics `tcp.active` and `pickle.active`
* Optional udp incomplete messages logging: `log-incomplete` setting
* Fixes for working on x86-32
* logging fsnotify: fix ONCE rotation bug

##### version 0.4.3
* Optional whisper throttle setting #8: `max-updates-per-second`

##### version 0.4.2
* Fix bug in go-whisper: points in long archives missed if metrics retention count >=3

##### version 0.4.1
* Bug fix schemas parser

##### version 0.4
* Code refactoring and improved test coverage (thanks [Dave Rawks](https://github.com/drawks))
* Bug fixes

##### version 0.3
* Log "create wsp" as debug
* Log UDP checkpoint (calculate stats every minute)
* Rotate logfile by inotify event (without HUP)
* Check logfile opened
* [storage-aggregation.conf](http://graphite.readthedocs.org/en/latest/config-carbon.html#storage-aggregation-conf) support
* Create and chown logfile before daemonize and change user
* Debian package (thanks [Dave Rawks](https://github.com/drawks))

##### version 0.2
* Git submodule dependencies
* Init script for CentOS 6
* Makefile
* "make rpm" script
* Daemonize and run-as-user support
* `-check-config` option
* `-pidfile` option

##### version 0.1
+ First full-functional public version
+ Logging with HUP rotation support
+ UDP receiver
+ Tcp receiver
+ Pickle receiver
+ TOML-configs
+ Carbonlink
+ Multi-persister support
+ storage-schemas.conf support
