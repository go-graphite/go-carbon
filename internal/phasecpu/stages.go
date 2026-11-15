package phasecpu

import "github.com/prometheus/client_golang/prometheus"

// PersisterStageNames bounds exported stage cardinality and shares the timer vocabulary.
var PersisterStageNames = [...]string{"total", "route", "lock_wait", "load", "tail_read", "decode", "encode", "ooo_merge", "commit", "original_write", "publish", "acknowledge", "cache_pop", "other"}

// PersisterStageSampleEvery is the timer's batch sampling denominator.
const PersisterStageSampleEvery = 8

var (
	stageCPUDesc     = prometheus.NewDesc("persister_stage_cpu_seconds_total", "Estimated on-CPU seconds by write stage; extrapolated across sampled batches and successful profiling intervals. Excludes asynchronous storage work.", []string{"stage"}, nil)
	stageSampledDesc = prometheus.NewDesc("persister_stage_sampled_cpu_seconds_total", "On-CPU seconds actually observed in labelled write batches during profiling windows, without extrapolation.", []string{"stage"}, nil)
)
