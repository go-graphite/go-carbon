## Changelog
##### master
* bump google.golang.org/grpc to fix vulnerability GHSA-m425-mq94-257g by @KacperLegowski in https://github.com/go-graphite/go-carbon/pull/574
* Actions bump and fpm fix by @deniszh in https://github.com/go-graphite/go-carbon/pull/580
* Add dependabot config by @RincewindsHat in https://github.com/go-graphite/go-carbon/pull/583
* Remove old otel dependency, upgrade deps by @deniszh in https://github.com/go-graphite/go-carbon/pull/586
* Adding HTTP GET handler for health check by @deniszh in https://github.com/go-graphite/go-carbon/pull/588
* Using cuckoo filter for new metric detection instead of cache by @deniszh in https://github.com/go-graphite/go-carbon/pull/590
* Let's not delete values from boom filter by @deniszh in https://github.com/go-graphite/go-carbon/pull/593
* fix: panic on slice bounds out of range when preparing data stream by @dowster in https://github.com/go-graphite/go-carbon/pull/599
* Speed up fetchData by @deniszh in https://github.com/go-graphite/go-carbon/pull/601
* Make throughput quota config per minute by @emadolsky in https://github.com/go-graphite/go-carbon/pull/612

###### dependabot updates
* Bump github/codeql-action from 2 to 3 by @dependabot in https://github.com/go-graphite/go-carbon/pull/584
* Bump golangci/golangci-lint-action from 4 to 5 by @dependabot in https://github.com/go-graphite/go-carbon/pull/585
* Bump golangci/golangci-lint-action from 5 to 6 by @dependabot in https://github.com/go-graphite/go-carbon/pull/587
* Bump docker/build-push-action from 5 to 6 by @dependabot in https://github.com/go-graphite/go-carbon/pull/598
* Bump google.golang.org/grpc from 1.64.0 to 1.64.1 by @dependabot in https://github.com/go-graphite/go-carbon/pull/600
* Bump google.golang.org/grpc from 1.64.1 to 1.65.0 by @dependabot in https://github.com/go-graphite/go-carbon/pull/602
* Bump github.com/BurntSushi/toml from 1.3.2 to 1.4.0 by @dependabot in https://github.com/go-graphite/go-carbon/pull/603
* Bump github.com/klauspost/compress from 1.17.8 to 1.17.9 by @dependabot in https://github.com/go-graphite/go-carbon/pull/604
* Bump google.golang.org/protobuf from 1.34.1 to 1.34.2 by @dependabot in https://github.com/go-graphite/go-carbon/pull/605
* Bump cloud.google.com/go/pubsub from 1.38.0 to 1.40.0 by @dependabot in https://github.com/go-graphite/go-carbon/pull/606
* Bump golang.org/x/net from 0.26.0 to 0.27.0 by @dependabot in https://github.com/go-graphite/go-carbon/pull/607
* Bump google.golang.org/api from 0.181.0 to 0.188.0 by @dependabot in https://github.com/go-graphite/go-carbon/pull/608
* Bump google.golang.org/api from 0.188.0 to 0.190.0 by @dependabot in https://github.com/go-graphite/go-carbon/pull/610
* Bump cloud.google.com/go/pubsub from 1.40.0 to 1.41.0 by @dependabot in https://github.com/go-graphite/go-carbon/pull/611
* Bump google.golang.org/api from 0.190.0 to 0.191.0 by @dependabot in https://github.com/go-graphite/go-carbon/pull/613
* Bump golang.org/x/net from 0.27.0 to 0.28.0 by @dependabot in https://github.com/go-graphite/go-carbon/pull/614
* Bump github.com/prometheus/client_golang from 1.19.1 to 1.20.0 by @dependabot in https://github.com/go-graphite/go-carbon/pull/616
* Bump google.golang.org/api from 0.191.0 to 0.192.0 by @dependabot in https://github.com/go-graphite/go-carbon/pull/617
* Bump github.com/IBM/sarama from 1.43.2 to 1.43.3 by @dependabot in https://github.com/go-graphite/go-carbon/pull/618

##### version 0.17.3
* Bump golang.org/x/net from 0.7.0 to 0.17.0 by @dependabot in #568
* Version bump by @deniszh

##### version 0.17.2
* Refresh actions and go versions by @deniszh in #539
* Use protojson.Marshal for produce json from the proto messages by @deniszh in #540
* update go-whisper lib by @auguzun in #542
* Fixing typo with go tip, update Docker base images by @deniszh in #543
* updated golang in go.mod from 1.18 to 1.20 by @auguzun in #545
* added logic to handle corrupt whisper file without archive info by @auguzun in #548
* Dependabots security PRs by @deniszh in #551
* Bump golang.org/x/net from 0.0.0-20210525063256-abc453219eb5 to 0.7.0 by @dependabot in #549
* add shared lock for read request by @auguzun in #553
* removed trigram field in trie index because it is not used by @auguzun in #554
* Update go-whisper by @deniszh in #560
* Optionally disable 404 error logging (fix #563) by @deniszh in #564
* Bump gopkg.in/yaml.v3 from 3.0.0-20200313102051-9f266ea9e77c to 3.0.0 by @dependabot in #565
* Build packages for Debian Bookworm by @anayrat in #571

##### version 0.17.1
* Version bump by @deniszh

##### version 0.17.0
* Make empty results ok #453
* carbonserver: /list_query?leaft_only=true and /admin/info?scopes=config #454
* Makefile: revert .SHELLFLAGS changes #456
* Upgrade to go-1.18 #460
* Removing DockerHub upload #461
* Introducing `persiter.oooDiscardedPoints` metric #463
* Fixing OOO discard metric overflow #464
* refactoring `persiter.oooDiscardedPoints` metric #TBD
* __Online Schema/Aggregation Migration__ #438
* tool/persister_configs_differ and -check-policies flag in go-carbon #438
* Make empty results ok by @jdblack in https://github.com/go-graphite/go-carbon/pull/
* carbonserver: /list_query?leaft_only=true  and /admin/info?scopes=config by @bom-d-van in https://github.com/go-graphite/go-carbon/pull/454
* Makefile: revert .SHELLFLAGS changes by @bom-d-van in https://github.com/go-graphite/go-carbon/pull/456
* Upgrade to go-1.18 by @emadolsky in https://github.com/go-graphite/go-carbon/pull/460
* Removing DockerHub upload by @deniszh in https://github.com/go-graphite/go-carbon/pull/461
* Introducing `persiter.oooDiscardedPoints` metric by @deniszh in https://github.com/go-graphite/go-carbon/pull/463
* Fixing OOO discard metric overflow by @deniszh in https://github.com/go-graphite/go-carbon/pull/464
* Refactoring persiter.oooDiscardedPoints metric by @deniszh in https://github.com/go-graphite/go-carbon/pull/465
* persister: online schema migration by @bom-d-van in https://github.com/go-graphite/go-carbon/pull/438
* quota: throughput racy enforcement bug fixes by @bom-d-van in https://github.com/go-graphite/go-carbon/pull/467
* carbonserver: introducing request-timeout, heavy-glob-query-rate-limiters and api-per-path-rate-limiters for read traffic regulation by @bom-d-van in https://github.com/go-graphite/go-carbon/pull/469
* quota: add two unit tests for proper enforcement by @bom-d-van in https://github.com/go-graphite/go-carbon/pull/468
* carbonserver: introduce file list cache v2 by @bom-d-van in https://github.com/go-graphite/go-carbon/pull/470
* persister: update go-whisper for cwhisper appendToBlockAndRotate bug fix by @bom-d-van in https://github.com/go-graphite/go-carbon/pull/478
* protocol: upgrade to the latest version by @bom-d-van in https://github.com/go-graphite/go-carbon/pull/472
* CarbonV2 gRPC streaming render by @emadolsky in https://github.com/go-graphite/go-carbon/pull/476
* Docker build from local copy, expands /var/lib/graphite/ paths in Doc… by @flucrezia in https://github.com/go-graphite/go-carbon/pull/481
* Add stats for find and render requests by @auguzun in https://github.com/go-graphite/go-carbon/pull/482
* Fixing panic in carbonserver by @deniszh in https://github.com/go-graphite/go-carbon/pull/485
* gRPC interceptors by @emadolsky in https://github.com/go-graphite/go-carbon/pull/483
* Proper enrichFromCache  panic fix and go-whisper upgrade by @deniszh in https://github.com/go-graphite/go-carbon/pull/486
* add into access log "complexity" of find request by @enuret in https://github.com/go-graphite/go-carbon/pull/487
* Find grpc by @emadolsky in https://github.com/go-graphite/go-carbon/pull/488
* Info grpc by @emadolsky in https://github.com/go-graphite/go-carbon/pull/489
* Add gRPC metadata for getting carbonapi_uuid by @emadolsky in https://github.com/go-graphite/go-carbon/pull/490
* Factor for physical size for sparse mode. For sparse mode I added config variable which applies to logical size… by @auguzun in https://github.com/go-graphite/go-carbon/pull/491
* Add simple cache for grpc render by @emadolsky in https://github.com/go-graphite/go-carbon/pull/492
* Fixed a bug with trie index recreation from the cache by @auguzun in https://github.com/go-graphite/go-carbon/pull/493
* Reduce useless logs by @emadolsky in https://github.com/go-graphite/go-carbon/pull/495
* Fix deepsource issues by @emadolsky in https://github.com/go-graphite/go-carbon/pull/496
* Fix carbonserver Stat() from resetting the value of max-inflight-requests to zero by @jmeichle in https://github.com/go-graphite/go-carbon/pull/497
* Add 'streaming-query-cache-enabled' config param by @emadolsky in https://github.com/go-graphite/go-carbon/pull/498
* Solved problem with different number of metrics in trie index and on disk by @auguzun in https://github.com/go-graphite/go-carbon/pull/499
* carbonserver: fix a cache hit bug by @cxfcxf in https://github.com/go-graphite/go-carbon/pull/494
* Revert "Solved problem with different number of metrics in trie index and on disk" by @auguzun in https://github.com/go-graphite/go-carbon/pull/500
* Fixed the problem with different number of metrics in trie index and on disk by @auguzun in https://github.com/go-graphite/go-carbon/pull/501
* Removed metric newMetricCount by @auguzun in https://github.com/go-graphite/go-carbon/pull/502
* Fix request duration bucket metrics naming by @emadolsky in https://github.com/go-graphite/go-carbon/pull/503
* Calculate and add fetch size in gRPC render by @emadolsky in https://github.com/go-graphite/go-carbon/pull/508
* Add carbonserver render tracing by @emadolsky in https://github.com/go-graphite/go-carbon/pull/509
* Add stats to render trace logs by @emadolsky in https://github.com/go-graphite/go-carbon/pull/510
* Add keepalive server parameters & enforcements by @emadolsky in https://github.com/go-graphite/go-carbon/pull/511
* Don't use find cache for not founds in grpc by @emadolsky in https://github.com/go-graphite/go-carbon/pull/512
* Add gRPC gzip compression to carbonserver by @emadolsky in https://github.com/go-graphite/go-carbon/pull/513
* Add gRPC initial win size of 4MB for less latency by @emadolsky in https://github.com/go-graphite/go-carbon/pull/514
* Use find cache for glob expansion in grpc render by @emadolsky in https://github.com/go-graphite/go-carbon/pull/516
* Fixed index panic during metric fetch on corrupt file by @auguzun in https://github.com/go-graphite/go-carbon/pull/519
* optimisation(carbonserver): separate grpc expandedGlobsCache from findCache into a separate one, and restore response caching in findCache; and use expandedGlobsCache in http find/render by @timtofan in https://github.com/go-graphite/go-carbon/pull/520
* fix(carbonserver): find http/grpc - fix metrics_found metric by @timtofan in https://github.com/go-graphite/go-carbon/pull/521
* cleanup: remove <requestType>Errors metrics in favour of status_codes.* ones as more reliable by @timtofan in https://github.com/go-graphite/go-carbon/pull/523
* Use intermediate chan to expedite gRPC render cache by @emadolsky in https://github.com/go-graphite/go-carbon/pull/522
* Decrease gRPC streaming channel size by @emadolsky in https://github.com/go-graphite/go-carbon/pull/524
* Limit streaming channel size dynamically for gRPC render by @emadolsky in https://github.com/go-graphite/go-carbon/pull/526
* added dockerfile.debug and enabled carbonserver in test config by @timtofan in https://github.com/go-graphite/go-carbon/pull/527
* fix(carbonserver): find - cache http404 responses, as render handler does by @timtofan in https://github.com/go-graphite/go-carbon/pull/528
* fix(carbonserver): grpc find - avoid unnecessary glob expansions upon responseCache hit by @timtofan in https://github.com/go-graphite/go-carbon/pull/529
* find - rename stat metrics_found to metrics_found_without_response_cache to clarify its meaning by @timtofan in https://github.com/go-graphite/go-carbon/pull/530
* find - rename stat metrics_found_without_response_cache to find_metrics_found_without_response_cache to denote that it's only for find handler by @timtofan in https://github.com/go-graphite/go-carbon/pull/531
* Fix uninitialized render stream chan on cached res by @emadolsky in https://github.com/go-graphite/go-carbon/pull/532
* fix(find): in http set ErrNotFound in codepath when findCache is disabled; in grpc exit early if expandGlobs failed by @timtofan in https://github.com/go-graphite/go-carbon/pull/533
* added metric for ooo lag for each datapoint by @auguzun in https://github.com/go-graphite/go-carbon/pull/534
* Bump github.com/prometheus/client_golang from 0.9.1 to 1.11.1 by @dependabot in https://github.com/go-graphite/go-carbon/pull/536

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
