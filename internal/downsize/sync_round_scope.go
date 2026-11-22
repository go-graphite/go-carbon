package downsize

import "fmt"

func (o syncOptions) roundLimit() int {
	// Legacy saved configurations contain max-rounds=0. Upgrading those
	// finish-first passes must be bounded too, without editing their state.
	if o.maxRounds == 0 && !o.rescanAll {
		return 2
	}
	return o.maxRounds
}

// Journal the scheduler's input before entering a round. Without this receipt,
// legacy frontier rounds already entered used the union; preserve that input
// across an upgrade, including rounds replayed to reconstruct the repair set.
func (s *syncer) roundUsesUnion(round int, scoped bool) (bool, error) {
	journalRound := round + s.verificationBase
	if !s.o.rescanAll && s.legacyRepairScope(journalRound) != nil {
		return false, nil
	}
	for _, row := range s.j.rows {
		if row.Kind == "verification-round-scope" && row.Round == journalRound {
			switch row.Phase {
			case "union":
				return true, nil
			case "repairs":
				return false, nil
			default:
				return false, fmt.Errorf("invalid verification round scope %q", row.Phase)
			}
		}
	}
	legacyEntered := (s.resume.Phase == "verify" || s.resume.Phase == "fill") && journalRound <= s.resume.Round
	full := !scoped && (round == 1 || s.frontier != nil && (s.o.rescanAll || legacyEntered))
	phase := "repairs"
	if full {
		phase = "union"
	}
	return full, s.j.append(syncRow{Kind: "verification-round-scope", Round: journalRound, Phase: phase})
}
