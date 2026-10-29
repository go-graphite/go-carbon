// Package phasecpu attributes sampled process CPU to a fixed set of subsystems.
package phasecpu

import (
	"context"
	"runtime/pprof"
)

type Phase uint8

const (
	Other Phase = iota
	ReceiverTCP
	ReceiverUDP
	ReceiverPickle
	ReceiverProtobuf
	ReceiverHTTP
	ReceiverKafka
	ReceiverPubSub
	CacheInsert
	WriteoutQueue
	Persister
	PebbleFlush
	PebbleCompaction
	PebbleWAL
	PebbleTableCache
	PebbleGC
	InputLog
	Find
	Render
	Info
	Index
	Quota
	Buckyd
	Reclaim
	MetadataCache
	Restore
	Backfill
	ProfileWork
	numPhases
)

var names = [numPhases]string{
	"other", "receiver_tcp", "receiver_udp", "receiver_pickle", "receiver_protobuf",
	"receiver_http", "receiver_kafka", "receiver_pubsub", "cache_insert", "writeout_queue",
	"persister", "pebble_flush", "pebble_compaction", "pebble_wal", "pebble_table_cache",
	"pebble_gc", "input_log", "carbonserver_find", "carbonserver_render", "carbonserver_info",
	"carbonserver_index", "quota", "buckyd", "reclaim", "metadata_cache", "restore", "backfill", "phase_profiler",
}

func (p Phase) String() string { return names[p] }

var contexts = func() [numPhases]context.Context {
	var out [numPhases]context.Context
	for p := range numPhases {
		out[p] = pprof.WithLabels(context.Background(), pprof.Labels("go_carbon_phase", names[p]))
	}
	return out
}()

func Context(p Phase) context.Context { return contexts[p] }

// Set labels a goroutine and its future children. Use at subsystem entry points;
// Pebble's own labels take precedence when accounting for its background work.
func Set(p Phase) { pprof.SetGoroutineLabels(contexts[p]) }

func Receiver(protocol string) Phase {
	switch protocol {
	case "tcp":
		return ReceiverTCP
	case "udp":
		return ReceiverUDP
	case "pickle":
		return ReceiverPickle
	case "protobuf":
		return ReceiverProtobuf
	case "http":
		return ReceiverHTTP
	case "kafka":
		return ReceiverKafka
	case "pubsub":
		return ReceiverPubSub
	default:
		return Other
	}
}
