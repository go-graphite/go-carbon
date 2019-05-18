# Go Whisper

[![Build Status](https://travis-ci.org/robyoung/go-whisper.png?branch=master)](https://travis-ci.org/robyoung/go-whisper?branch=master)

Go Whisper is a [Go](http://golang.org/) implementation of the [Whisper](https://github.com/graphite-project/whisper) database, which is part of the [Graphite Project](http://graphite.wikidot.com/).

To create a new whisper database you must define it's retention levels (see: [storage schemas](http://graphite.readthedocs.org/en/1.0/config-carbon.html#storage-schemas-conf)), aggregation method and the xFilesFactor. The xFilesFactor specifies the fraction of data points in a propagation interval that must have known values for a propagation to occur.

## Examples

Create a new whisper database in "/tmp/test.wsp" with two retention levels (1 second for 1 day and 1 hour for 5 weeks), it will sum values when propagating them to the next retention level, and it requires half the values of the first retention level to be set before they are propagated.
```go
retentions, err := whisper.ParseRetentionDefs("1s:1d,1h:5w")
if err == nil {
  wsp, err := whisper.Create("/tmp/test.wsp", retentions, whisper.Sum, 0.5)
}
```

Alternatively you can open an existing whisper database.
```go
wsp, err := whisper.Open("/tmp/test.wsp")
```

Once you have a whisper database you can set values at given time points. This sets the time point 1 hour ago to 12345.678.
```go
wsp.Update(12345.678, time.Now().Add(time.ParseDuration("-1h")).Unix())
```

And you can retrieve time series from it. This example fetches a time series for the last 1 hour and then iterates through it's points.
```go
series, err := wsp.Fetch(time.Now().Add(time.ParseDuration("-1h")).Unix(), time.Now().Unix())
if err != nil {
  // handle
}
for _, point := range series.Points() {
  fmt.Println(point.Time, point.Value)
}
```

## Thread Safety

This implementation is *not* thread safe. Writing to a database concurrently will cause bad things to happen. It is up to the user to manage this in their application as they need to.

## Compressed Format

go-whisper library supports a compressed format, which maintains the same functionality of standard whisper file, while keeping data in a much smaller size. This compressed format is called `cwhisper`.

Compression algorithm source: [4.1 Time series compression in Gorilla: A Fast, Scalable, In-Memory Time Series Database](https://www.vldb.org/pvldb/vol8/p1816-teller.pdf).

Data point in cwhisper ranges from 2 - 14 bytes (12 bytes for standard format). So in theory, cwhisper file size could be 16.67% - 116.67% of standard file size. So the theoretical compression ratio is 6 - 0.86.

In random data point testing, compressed/uncompressed ratio is between 18.88% and 113.25%.

In real production payload, we are seeing 50%+ less disk space usage.

Read/Write Performance between standard and compressed formats:

```
BenchmarkWriteStandard-8           	   50000	     33824 ns/op
BenchmarkWriteCompressed-8         	 1000000	      1630 ns/op

BenchmarkReadStandard-8            	     500	   2270392 ns/op
BenchmarkReadCompressed-8          	   10000	    260862 ns/op
```

### Drawbacks

* cwhisper is faster and smaller, but unlike standard format, you can't easily backfill/update/rewrite old data points because it's not data-point addressable.
* file size could grow if data points are irregular.

### Suitable Application

* cwhisper is most suitable for metrics that are mostly regular and less likely needed to backfill/rewrite old data, like system metrics. cwhisper also works nicely for sparse metrics.

### How does it work in a nutshell

An example format: https://github.com/go-graphite/go-whisper/blob/master/doc/compressed.md

In cwhisper, archives are broken down into multiple blocks (by default 7200 data points per block as recommended by the gorilla paper), and data points are compressed into blocks. cwhisper assumes 2 as the default data point size, but when it detects that the default size is too small, it would grow the file.

cwhisper still has one file per metric, it's doing round-robin update, instead of rotating data points, block is rotation unit for archives.

## Licence

Go Whisper is licenced under a BSD Licence.
