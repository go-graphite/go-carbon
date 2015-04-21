go-carbon [![Build Status](https://travis-ci.org/lomik/go-carbon.svg?branch=master)](https://travis-ci.org/lomik/go-carbon)
============

Golang implementation of Graphite/Carbon server with classic architecture: Agent -> Cache -> Persister

![Architecture](doc/design.png)

### Supported features
* Receive metrics from TCP and UDP ([plaintext protocol](http://graphite.readthedocs.org/en/latest/feeding-carbon.html#the-plaintext-protocol))
* Receive metrics with [Pickle protocol](http://graphite.readthedocs.org/en/latest/feeding-carbon.html#the-pickle-protocol) (TCP only)
* [storage-schemas.conf](http://graphite.readthedocs.org/en/latest/config-carbon.html#storage-schemas-conf)
* [storage-aggregation.conf](http://graphite.readthedocs.org/en/latest/config-carbon.html#storage-aggregation-conf)
* Carbonlink (requests to cache from graphite-web)
* Logging with rotation (reopen log by HUP signal or inotify event)
* Many persister workers (using many cpu cores)
* Run as daemon

## Performance

Faster than default carbon. In all conditions :) How much faster depends on server hardware, storage-schemas, etc.

The result of replacing "carbon" to "go-carbon" on a server with a load up to 900 thousand metric per minute:

![Success story](doc/success1.png)

## Installation
```
# build binary
git clone https://github.com/lomik/go-carbon.git
cd go-carbon
make submodules
make

# build rpm (centos 6)
make rpm

# build debian/ubuntu package
make deb

# hand-made install
sudo install -m 0755 go-carbon /usr/local/bin/go-carbon
sudo go-carbon --config-print-default > /usr/local/etc/carbon.conf
sudo vim /usr/local/etc/carbon.conf
sudo go-carbon --config /usr/local/etc/carbon.conf --daemon
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

```
[common]
# Run as user. Works only in daemon mode
user = ""
# If logfile is empty use stderr
logfile = "/var/log/go-carbon/go-carbon.log"
# Prefix for store all internal go-carbon graphs. Supported macroses: {host}
graph-prefix = "carbon.agents.{host}."
# Increase for configuration with multi persisters
max-cpu = 1

[whisper]
data-dir = "/data/graphite/whisper/"
# http://graphite.readthedocs.org/en/latest/config-carbon.html#storage-schemas-conf. Required
schemas-file = "/data/graphite/schemas"
# http://graphite.readthedocs.org/en/latest/config-carbon.html#storage-aggregation-conf. Optional
aggregation-file = ""
# Workers count. Metrics sharded by "crc32(metricName) % workers"
workers = 1
enabled = true

[cache]
# Limit of in-memory stored points (not metrics)
max-size = 1000000
# Capacity of queue between receivers and cache
input-buffer = 51200

[udp]
listen = ":2003"
enabled = true

[tcp]
listen = ":2003"
enabled = true

[pickle]
listen = ":2004"
enabled = true

[carbonlink]
listen = "127.0.0.1:7002"
enabled = true
# Close inactive connections after "read-timeout"
read-timeout = "30s"
# Return empty result if cache not reply
query-timeout = "100ms"

[pprof]
listen = "localhost:7007"
enabled = false
```

## Changelog
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
* "--check-config" option
* "--pidfile" option

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
