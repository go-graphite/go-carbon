package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
)

// replayCorpus is a durable, journal-identity-bound synthetic name/policy
// catalog. It describes all metrics before any row is appended, including each
// original one-point apply group's order and clock. It is never inferred from a
// numeric-only journal after a crash and is not a production capture format.
type replayCorpus struct {
	Version                           int
	Identity                          string
	Historical, Metrics               uint64
	Producers                         int
	Schemas                           []string
	NameFormat, PolicyRule, ApplyRule string
}

func corpusDefinition(identity string, total, metrics uint64, producers int) replayCorpus {
	return replayCorpus{1, identity, total, metrics, producers, combinedSchemas,
		"corpus.metric.%08d", "schema=(id/6)%70,method=id%6+1,xff=0.5",
		"one-point-batch,order=sequence+1,clock=timestamp,historical-time=1800000000+sequence/6000000,live-time=1800000000+sequence/20000000"}
}
func writeReplayCorpus(root, identity string, total, metrics uint64, producers int) error {
	if metrics%uint64(producers) != 0 {
		return errors.New("metadata corpus requires one producer per metric")
	}
	data, err := json.Marshal(corpusDefinition(identity, total, metrics, producers))
	if err != nil {
		return err
	}
	f, err := os.OpenFile(filepath.Join(root, "corpus.json"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	_, err = f.Write(data)
	err = errors.Join(err, f.Sync(), f.Close())
	if err != nil {
		return err
	}
	d, err := os.Open(root)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
func readReplayCorpus(root string) (replayCorpus, error) {
	var c replayCorpus
	data, err := os.ReadFile(filepath.Join(root, "corpus.json"))
	if err != nil {
		return c, err
	}
	if err = json.Unmarshal(data, &c); err != nil {
		return c, err
	}
	var identity struct{ ID string }
	data, err = os.ReadFile(filepath.Join(root, "log", "IDENTITY"))
	if err != nil {
		return c, err
	}
	if err = json.Unmarshal(data, &identity); err != nil {
		return c, err
	}
	if c.Metrics == 0 || c.Producers < 1 || c.Metrics%uint64(c.Producers) != 0 || !reflect.DeepEqual(c, corpusDefinition(identity.ID, c.Historical, c.Metrics, c.Producers)) {
		return c, fmt.Errorf("invalid or mismatched synthetic metadata catalog")
	}
	return c, nil
}

var combinedSchemas = []string{
	"1s:1d", "10s:3d", "1s:2d", "1m:35d", "5m:90d", "15m:180d", "30m:1y", "1h:2y", "1d:10y",
	"1s:1d,1m:30d,1h:1y,1d:10y", "1m:30d,1h:1y,1d:10y", "1m:14d,30m:2y", "1s:3d,1m:14d,30m:2y",
	"10s:2d,1m:14d,30m:2y", "10m:1y,1d:10y", "1s:2d,1m:28d,1h:2y", "1s:2d,1m:62d,10m:5y",
	"10m:2d,1h:9d,1d:3y", "10s:4d,1m:31d,30m:4y", "1m:7d,5m:60d,1h:2y", "1m:14d,5m:60d,1h:2y",
	"1m:62d,30m:2y", "1h:90d,1d:2y", "1s:7d,1m:14d,30m:2y", "1s:1d,1m:30d,1h:2y",
	"1s:3d,1m:14d,30m:2y", "5m:60d,30m:2y", "10m:7d,1h:5y,1d:10y", "1m:14d,1h:5y",
	"10s:15d,1m:100d,15m:2y", "1m:200d,1h:5y", "1m:400d,1d:10y", "1m:400d,1h:5y",
	"1m:100d,30m:2y", "5m:400d,1h:2y", "1m:30d,15m:400d,1h:5y", "5m:400d,1h:5y",
	"5s:15d,1m:100d,15m:5y", "2s:7d,1m:100d,15m:2y", "1s:40d,1m:100d,15m:2y",
	"5m:14d,1d:10y", "1h:90d,1d:10y",
	"10m:30d",
	"12h:10y",
	"15m:1y",
	"15m:840d",
	"1d:2y",
	"1d:3y",
	"1d:5y",
	"1d:90d",
	"1h:10y",
	"1h:15y",
	"1h:1y",
	"1h:30d",
	"1h:5y",
	"1m:14d",
	"1m:14d,5m:60d,60m:2y",
	"1m:25y",
	"1m:2d,1h:1y,1d:10y",
	"1m:30d",
	"1m:7d,5m:60d,60m:2y",
	"1m:90d",
	"1s:7d",
	"1w:10y",
	"2h:10y",
	"30m:30d",
	"30m:3y",
	"5m:1y",
	"5m:30d",
	"5m:420d",
}
