package v2daemon

import (
	"encoding/binary"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/go-graphite/go-carbon/internal/partsformat"
	"github.com/go-graphite/go-carbon/internal/sealrollup"
	"github.com/go-graphite/go-carbon/internal/v2core"
	whisper "github.com/go-graphite/go-whisper"
)

type fixtureArchiveSlot struct {
	archive int
	slot    int64
	stamp   int64
}

// The fixture oracle derives occupancy/timestamps from actual classic files,
// using exactly the singleton groups and clocks written to the journal. Its
// constant dyadic values make the expected Average bits identical for all names.
func classicFixtureSlots(f Fixture, shape int) ([]fixtureArchiveSlot, error) {
	cfg := fixturePolicy(shape)
	var defs []string
	for _, a := range cfg.Archives {
		defs = append(defs, fmt.Sprintf("%ds:%ds", a.Step, a.Step*a.Count))
	}
	directory, err := os.MkdirTemp("", "v2-classic-oracle-")
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(directory)
	path := filepath.Join(directory, "classic.wsp")
	clock := int64(1794700800)
	classic, err := whisper.CreateWithOptions(path, whisper.MustParseRetentionDefs(strings.Join(defs, ",")), whisper.Average, .5, &whisper.Options{Sparse: true, Now: func() time.Time { return time.Unix(clock, 0) }})
	if err != nil {
		return nil, err
	}
	for round := 0; round < f.Rounds; round++ {
		clock = 1794700800 + int64((round/4+1)*4)
		point := fixturePoint(shape, round)
		if err = classic.UpdateMany([]*whisper.TimeSeriesPoint{{Time: int(point.Timestamp), Value: point.Value}}); err != nil {
			_ = classic.Close()
			return nil, err
		}
	}
	if err = classic.Close(); err != nil {
		return nil, err
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var out []fixtureArchiveSlot
	for ai, a := range cfg.Archives {
		offset := int(binary.BigEndian.Uint32(data[16+12*ai:]))
		raw := data[offset : offset+int(a.Count)*12]
		for i := 0; i < len(raw); i += 12 {
			stamp := int64(binary.BigEndian.Uint32(raw[i:]))
			if stamp != 0 {
				out = append(out, fixtureArchiveSlot{ai, (stamp / a.Step) % a.Count, stamp})
			}
		}
	}
	return out, nil
}

func verifyFixedRollups(reader *v2core.Engine, f Fixture, names []string) (int, uint64, error) {
	var templates [4][]fixtureArchiveSlot
	for i := range templates {
		var err error
		templates[i], err = classicFixtureSlots(f, i)
		if err != nil {
			return 0, 0, err
		}
	}
	seen := make([]bool, f.Names)
	count := 0
	slots := uint64(0)
	for _, db := range reader.DBs() {
		snapshot := db.NewSnapshot()
		err := v2core.ScanRollups(snapshot, func(p v2core.RollupPolicy, m v2core.RollupMetadata, records []sealrollup.Record) error {
			id := int(m.ID) - 1
			if id < 0 || id >= f.Names || seen[id] || string(p.Name) != names[id] || p.Group != partsformat.Group("bench") {
				return fmt.Errorf("invalid archive identity %s", p.Name)
			}
			seen[id] = true
			count++
			producer := id % f.Producers
			width := (f.Names-1-producer)/f.Producers + 1
			local := id / f.Producers
			expectedOrder := uint64((f.Rounds-1)*width + local + 1)
			if m.Frontier.Order != expectedOrder || m.Frontier.MaxTime != fixturePoint(id, f.Rounds-1).Timestamp || m.Revision != uint64(f.Rounds+1) || m.LastUpdate != (1794700800+int64(f.Rounds))*1e9 {
				return fmt.Errorf("archive metadata mismatch id%d %+v", id, m)
			}
			want := templates[id%4]
			if len(records) != len(want) {
				return fmt.Errorf("archive slot count id%d got%d want%d", id, len(records), len(want))
			}
			expected := make(map[[2]int64]int64, len(want))
			for _, s := range want {
				expected[[2]int64{int64(s.archive), s.slot}] = s.stamp
			}
			for _, record := range records {
				stamp, ok := expected[[2]int64{int64(record.Archive), record.Slot}]
				lastRound := min(f.Rounds-1, int(stamp+p.Config.Archives[record.Archive].Step-1-1794700799))
				order := uint64(lastRound*width + local + 1)
				orderOK := record.Order == order
				if len(p.Config.Archives) == 1 {
					// A certified run stores its maximum as a representative,
					// bounded by the exact row clock and this name's frontier.
					orderOK = record.Order >= order && record.Order <= m.Frontier.Order
				}
				if !ok || record.Sample.Time != stamp || record.Sample.Bits != math.Float64bits(fixturePoint(id, 0).Value) || !orderOK || record.Sample.Sequence != record.Order {
					return fmt.Errorf("archive replacement mismatch id%s a%d slot%d got%+v wantstamp%d order%d", strconv.Itoa(id), record.Archive, record.Slot, record, stamp, order)
				}
			}
			slots += uint64(len(records))
			return nil
		})
		_ = snapshot.Close()
		if err != nil {
			return count, slots, err
		}
	}
	if count != f.Names {
		return count, slots, fmt.Errorf("archive names%d want%d", count, f.Names)
	}
	return count, slots, nil
}
