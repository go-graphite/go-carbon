// expiration-backfill repairs conversion activity through the local admin API.
package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"time"

	"github.com/go-graphite/go-carbon/buckyd"
	"github.com/go-graphite/go-carbon/internal/chunkstore"
)

type checkpoint struct {
	Identity string
	After    string
	Records  int
	Totals   buckyd.ActivityBackfillResult
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
func run() error {
	endpoint := flag.String("url", "http://127.0.0.1:4242", "buckyd admin URL")
	startText := flag.String("conversion-start", "", "conversion window start, RFC3339")
	endText := flag.String("conversion-end", "", "conversion window end, RFC3339")
	baselinePath := flag.String("baseline", "", "JSONL ActivityBaseline records from an immutable conversion-era catalog")
	progressPath := flag.String("checkpoint", "expiration-backfill.json", "resumable checkpoint (use a separate file for apply)")
	apply := flag.Bool("apply", false, "lower activity; requires a historical baseline")
	rate := flag.Int("rate", 100, "maximum scanned or changed records per second")
	flag.Parse()
	start, err := time.Parse(time.RFC3339, *startText)
	if err != nil {
		return err
	}
	end, err := time.Parse(time.RFC3339, *endText)
	if err != nil || !end.After(start) {
		return errors.New("invalid conversion end")
	}
	if *rate < 1 || (*apply && *baselinePath == "") {
		return errors.New("positive rate and a conversion-era baseline for apply are required")
	}
	var source *os.File
	var scanner *bufio.Scanner
	baselineHash := ""
	if *baselinePath != "" {
		source, err = os.Open(*baselinePath)
		if err != nil {
			return err
		}
		defer source.Close()
		hash := sha256.New()
		if _, err := io.Copy(hash, source); err != nil {
			return err
		}
		baselineHash = fmt.Sprintf("%x", hash.Sum(nil))
		if _, err := source.Seek(0, 0); err != nil {
			return err
		}
		scanner = bufio.NewScanner(source)
		scanner.Buffer(make([]byte, 4096), 1<<20)
	}
	identity := fmt.Sprintf("%s|%s|%s|%t|%s", *endpoint, start, end, *apply, baselineHash)
	state := checkpoint{Identity: identity, Totals: buckyd.ActivityBackfillResult{AgeBuckets: map[string]int{}}}
	if data, err := os.ReadFile(*progressPath); err == nil {
		if err := json.Unmarshal(data, &state); err != nil {
			return err
		}
		if state.Identity != identity {
			return errors.New("checkpoint belongs to another host, window, mode or baseline")
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if state.Records < 0 {
		return errors.New("invalid checkpoint record position")
	}
	if state.Totals.AgeBuckets == nil {
		state.Totals.AgeBuckets = map[string]int{}
	}
	if scanner != nil {
		for range state.Records {
			if !scanner.Scan() {
				return errors.New("checkpoint exceeds baseline")
			}
		}
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	client := &http.Client{Timeout: 2 * time.Minute}
	token := os.Getenv("BUCKYD_TOKEN")
	for !state.Totals.Done {
		request := buckyd.ActivityBackfillRequest{Start: start, End: end, After: state.After, Limit: min(128, *rate), Apply: *apply}
		if scanner != nil {
			for range request.Limit {
				if !scanner.Scan() {
					break
				}
				var baseline chunkstore.ActivityBaseline
				if err := json.Unmarshal(scanner.Bytes(), &baseline); err != nil {
					return err
				}
				request.Baselines = append(request.Baselines, baseline)
			}
			if err := scanner.Err(); err != nil {
				return err
			}
			if len(request.Baselines) == 0 {
				state.Totals.Done = true
				break
			}
		}
		data, err := json.Marshal(request)
		if err != nil {
			return err
		}
		started := time.Now()
		result, err := post(ctx, client, *endpoint+"/v1/activity-backfill", token, data)
		if err != nil {
			return err
		}
		if scanner != nil {
			state.Records += len(request.Baselines)
		} else {
			state.After = result.Next
			state.Totals.Done = result.Done
		}
		state.Totals.Scanned += result.Scanned
		state.Totals.WouldChange += result.WouldChange
		state.Totals.Changed += result.Changed
		state.Totals.Unchanged += result.Unchanged
		state.Totals.Conflicts += result.Conflicts
		for bucket, count := range result.AgeBuckets {
			state.Totals.AgeBuckets[bucket] += count
		}
		if err := save(*progressPath, state); err != nil {
			return err
		}
		summary, _ := json.Marshal(state.Totals)
		fmt.Println(string(summary))
		delay := time.Duration(float64(time.Second)*float64(result.Scanned)/float64(*rate)) - time.Since(started)
		if delay > 0 {
			timer := time.NewTimer(delay)
			select {
			case <-ctx.Done():
				timer.Stop()
				return ctx.Err()
			case <-timer.C:
			}
		}
	}
	return save(*progressPath, state)
}

func post(ctx context.Context, client *http.Client, url, token string, body []byte) (buckyd.ActivityBackfillResult, error) {
	var result buckyd.ActivityBackfillResult
	for {
		request, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
		if err != nil {
			return result, err
		}
		request.Header.Set("Content-Type", "application/json")
		if token != "" {
			request.Header.Set("X-Buckyd-Authorization", token)
		}
		response, err := client.Do(request)
		if err != nil {
			return result, err
		}
		data, err := io.ReadAll(io.LimitReader(response.Body, 2<<20))
		response.Body.Close()
		if err != nil {
			return result, err
		}
		if response.StatusCode == 429 {
			timer := time.NewTimer(5 * time.Second)
			select {
			case <-ctx.Done():
				timer.Stop()
				return result, ctx.Err()
			case <-timer.C:
			}
			continue
		}
		if response.StatusCode != 200 {
			return result, fmt.Errorf("admin HTTP %d: %s", response.StatusCode, data)
		}
		return result, json.Unmarshal(data, &result)
	}
}
func save(path string, state checkpoint) error {
	data, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return err
	}
	file, err := os.OpenFile(path+".next", os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	_, writeErr := file.Write(data)
	if err := errors.Join(writeErr, file.Sync(), file.Close()); err != nil {
		return err
	}
	return os.Rename(path+".next", path)
}
