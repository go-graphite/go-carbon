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

An compression example:

1s:2d
1m:28d
1h:2y

metric_header (63)
archive_0_header (96)
archive_1_header (96)
archive_2_header (96)
[
	archive_0_block_0_info (16)
	archive_0_block_1_info (16)
	...
	archive_0_block_23_info (16)

	archive_0_buffer (120)
]
[
	archive_1_block_0_info (16)
	archive_1_block_1_info (16)
	...
	archive_1_block_5_info (16)

	archive_1_buffer (120)
]
[
	archive_2_block_0_info (16)
	archive_2_block_1_info (16)
	archive_2_block_3_info (16)
]
[
	archive_0_block_0 (14400)
	archive_0_block_1 (14400)
	...
	archive_0_block_3 (14400)
]
[
	archive_1_block_0 (14400)
	archive_1_block_1 (14400)
	...
	archive_1_block_5 (14400)
]
[
	archive_2_block_0 (14400)
	archive_2_block_1 (14400)
	archive_2_block_3 (14400)
]

## Licence

Go Whisper is licenced under a BSD Licence.
