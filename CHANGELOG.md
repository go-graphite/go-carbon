## Changelog
##### master
* carbonserver: introduce new empty-result option to allow for empty results with carbonserver #TBD
* introducing `persiter.oooDiscardedPoints` metric
##### version 0.16.2
* Another attempt to fix issues with release upload #449

##### version 0.16.1
* Fix issues with upload #448

##### version 0.16.0
* Fixing packaging #447 
* carbonserver/quota: throughput racy counter fixes and refactoring #446 
* trie: stop indexing empty directory nodes #445 
* carbonserver: introduce new max-inflight-requests and no-service-when-index-is-not-ready configs #443
* Fix carbonserver render error race #442
* Pipeline improvements #441
* Add Kafka msgpack support #440 
* carbonserver: introduce new api /metrics/list_query #435
* carbonserver: adds /admin/info endpoint for returning internal info #433
* carbonserver: fix a 404 related race condition in findCache #431
* carbonserver: log more detailed errors about symlink and continue trie index despite error #427
* Resolves #422 - Include section name when unknown aggregation method is used #423
* ctrie: a bug fix for trigram statNodes #419
* mod: upgrade to go 1.16 and drops SOURCES from Makefile #417
* carbonserver: change error/debug log destination to access.log for Ca... #416
* carbonserver: change max-metrics-globbed and max-metrics-rendered def... #416
* trie: numerious optimization and bug fixes for concurrent and realtim... #416
* cache-scan: only init Cache.Shard.adds when the feature is enabled #416
* receiver/kafka: fix ci lint errors #413
* receiver/kafka: moves time.NewTicker out of for loop #413
* receiver/kafka: fix kafka connection leaks when failing to consume pa... #413
* Fixing build on riscv64 (#410) #411

##### version 0.15.6
* trie/realtime: set wspConfigRetriever when realtime index is enabled #395 #396
* Go v1.15.3

##### version 0.15.5
* carbonserver: fix incorrect hanlding of counting and indexing files in updateFileList #385

##### version 0.15.4
* trie/bug fixes and finally adds some simple fuzzing logics #383

##### version 0.15.3
* Go v1.15.3
* carbonserver: fix findError information lost due to unexported fields #380
* Upgrade golangci-lint #378
* A concurrent version of trie index (#334)
* carbonserver: fix two panics related to prometheus #374 (#376)
* Add service.version for all the traces (#373)
* trie: add a little documentation about efficient metric naming patterns (#370)
* Implementing linters (#368)
* Go v1.15.1 (#367)
* Tracing/Opentelemetry (#364)
* [travis-ci] Add apt-get update and go mod vendor to sync gox installation in Travis (#361)
* Fix pubsub test (#360)
* fix packages.sh (#359)

##### version 0.15.0
* Migrate to go modules (#351)
* Moving to go-graphite organization (#347)
* Carbonlink support fixed with Python 3 Pickle Compatible Metric Request Parser (#340)
* (EXPERIMENTAL) Adding ability to update index for metrics even if there is no whisper file created on disk (#338)
* bugfix: unconfirmed metrics (#319)
* Implement context cancellations for find and find part of render (#307)
* (EXPERIMENTAL) Trie Index (#303)
* Populate metric details and access times only if stats are enabled (#299)
* Automatically delete empty whisper file caused by edge cases like server reboot (#293)
* (EXPERIMENTAL) Added new options and upgraded go-whisper library to have [compressed format](https://github.com/go-graphite/go-whisper#compressed-format) ([cwhisper](https://github.com/go-graphite/go-whisper/blob/master/doc/compressed.md)) support

##### version 0.14.0
* Accept UDP messages in plain protocol without trailing newline
* Added `whisper.hard-max-creates-per-second` option #242
* No longer trying to combine separate UDP messages from one sender into single stream
* [carbonserver] Added metrics for prometheus
* [carbonserver] Improved compatibility with graphite-web (#250, #251)

##### version 0.13.0
* Added `whisper.max-creates-per-second` option
* Support multiple targets in carbonserver
* Support new `carbonapi_v3_pb` protocol. This allows recent versions of carbonapi to get metadata alongside with data

##### version 0.12.0
* [Tags](http://graphite.readthedocs.io/en/latest/tags.html) support was added (only with [graphite-web](https://github.com/graphite-project/graphite-web))
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
