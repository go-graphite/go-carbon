package main

import (
	"errors"
	"math"
	"sort"
	"time"
)

type schedule struct {
	offsets []time.Duration
	cycle   time.Duration
	count   int64
	pos     int64
	fixed   time.Duration
}

type plan struct {
	Start        time.Time     `json:"source_start"`
	End          time.Time     `json:"source_end"`
	OriginalRPS  float64       `json:"original_rps"`
	RequestedRPS float64       `json:"requested_rps"`
	CappedRPS    float64       `json:"capped_rps_upper_bound"`
	Cycle        time.Duration `json:"replay_cycle_ns"`
}

func makeSchedule(requests []request, c config) (schedule, plan, error) {
	start, end := c.Start, c.End
	if start.IsZero() {
		start = requests[0].At
	}
	if end.IsZero() {
		// Include a final typical interarrival gap so looping does not double
		// the endpoint. Explicit [start,end) is preferable for sparse logs.
		gap := time.Second
		if len(requests) > 1 {
			gap = requests[len(requests)-1].At.Sub(requests[0].At) / time.Duration(len(requests)-1)
			if gap <= 0 {
				gap = time.Second
			}
		}
		end = requests[len(requests)-1].At.Add(gap)
	}
	span := end.Sub(start)
	if span <= 0 || span == time.Duration(math.MaxInt64) {
		return schedule{}, plan{}, errors.New("source window must have a finite positive span")
	}
	p := plan{Start: start, End: end, OriginalRPS: float64(len(requests)) / span.Seconds()}
	s := schedule{count: int64(len(requests))}
	if c.RPS > 0 {
		s.fixed = time.Duration(math.Ceil(float64(time.Second) / c.RPS))
		p.RequestedRPS = c.RPS
		if s.fixed < 1 || float64(s.count)*float64(s.fixed) >= float64(math.MaxInt64) {
			return schedule{}, p, errors.New("fixed-rate cycle outside duration range")
		}
		s.cycle = time.Duration(s.count) * s.fixed
	} else {
		cycle := float64(span) / c.Multiplier
		if cycle < 1 || cycle >= float64(math.MaxInt64) {
			return schedule{}, p, errors.New("scaled cycle outside duration range")
		}
		s.cycle = time.Duration(cycle)
		for _, r := range requests {
			offset := time.Duration(float64(r.At.Sub(start)) / c.Multiplier)
			if offset < 0 || offset >= s.cycle {
				return schedule{}, p, errors.New("request outside source cycle")
			}
			s.offsets = append(s.offsets, offset)
		}
		p.RequestedRPS = p.OriginalRPS * c.Multiplier
	}
	p.Cycle = s.cycle
	p.CappedRPS = math.Min(p.RequestedRPS, c.MaxRPS)
	return s, p, nil
}

func (s *schedule) next() time.Duration {
	if s.fixed > 0 {
		return time.Duration(s.pos) * s.fixed
	}
	return time.Duration(s.pos/s.count)*s.cycle + s.offsets[s.pos%s.count]
}

// take consumes the most recent due slot, dropping older slots in O(log N).
// The caller also spaces actual sends: a late wake can never cause a burst.
func (s *schedule) take(elapsed time.Duration) (index int, missed uint64) {
	latest := s.latest(elapsed)
	missed = uint64(latest - s.pos)
	s.pos = latest + 1
	return int(latest % s.count), missed
}

func (s *schedule) latest(elapsed time.Duration) int64 {
	if s.fixed > 0 {
		return int64(elapsed / s.fixed)
	}
	cycle, within := elapsed/s.cycle, elapsed%s.cycle
	i := sort.Search(len(s.offsets), func(i int) bool { return s.offsets[i] > within }) - 1
	return int64(cycle)*s.count + int64(i)
}
