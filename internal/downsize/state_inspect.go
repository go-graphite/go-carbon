package downsize

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
)

// StateMain only inspects the versioned indexed format. It cannot adopt,
// activate, or rewrite a legacy directory. Production activation is gated on
// complete adapters and legacy-reducer parity, rather than an operator flag.
func StateMain(ctx context.Context, args []string) error {
	return stateMain(ctx, args, os.Stdout)
}

type stateInspection struct {
	Version int                            `json:"version"`
	Unit    stateUnit                      `json:"unit"`
	Phases  []statePhase                   `json:"phases"`
	Windows []stateWindow                  `json:"windows"`
	NameID  int64                          `json:"name_id,omitempty"`
	Name    *syncName                      `json:"name,omitempty"`
	Issues  []stateIssue                   `json:"issues,omitempty"`
	Digests map[string]stateRelationDigest `json:"relation_digests,omitempty"`
}

func stateMain(ctx context.Context, args []string, out io.Writer) (err error) {
	if len(args) == 0 || args[0] != "inspect" {
		return errors.New("usage: storectl state inspect --database FILE [--id ID | --name NAME] [--hashes]")
	}
	f := flag.NewFlagSet("state inspect", flag.ContinueOnError)
	path := f.String("database", "", "indexed state database (opened read-only)")
	id := f.Int64("id", 0, "one-based immutable catalog ID")
	name := f.String("name", "", "exact metric name")
	hashes := f.Bool("hashes", false, "stream count and SHA256 for every semantic relation")
	if err := f.Parse(args[1:]); err != nil {
		return err
	}
	if *path == "" || f.NArg() != 0 || *id < 0 || *id != 0 && *name != "" {
		return errors.New("inspect requires --database and at most one of --id or --name")
	}
	s, err := openStateStore(*path, stateUnit{}, true)
	if err != nil {
		return fmt.Errorf("ERROR indexed state inspection: %w", err)
	}
	defer func() { err = errors.Join(err, s.close()) }()
	result := stateInspection{Version: stateSchema, Unit: s.unit, Phases: []statePhase{}, Windows: []stateWindow{}}
	if *name != "" {
		if err = s.db.QueryRowContext(ctx, "SELECT first_id FROM catalog_page WHERE first_name<=? ORDER BY first_name DESC LIMIT 1", *name).Scan(id); err != nil {
			return err
		}
		page, err := s.catalogPage("first_id=?", *id)
		if err != nil {
			return err
		}
		found := false
		for ordinal, entry := range page {
			if entry.Name == *name {
				*id += int64(ordinal)
				found = true
				break
			}
		}
		if !found {
			return sql.ErrNoRows
		}
	}
	if *id > 0 {
		n, err := s.catalogName(*id)
		if err != nil {
			return err
		}
		result.NameID, result.Name = *id, &n
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return err
	}
	defer tx.Rollback()
	rows, err := tx.QueryContext(ctx, "SELECT scope,phase,round,done,total,complete FROM phase ORDER BY scope")
	if err != nil {
		return err
	}
	for rows.Next() {
		var p statePhase
		if err = rows.Scan(&p.Scope, &p.Phase, &p.Round, &p.Done, &p.Total, &p.Complete); err != nil {
			rows.Close()
			return err
		}
		result.Phases = append(result.Phases, p)
	}
	if err = errors.Join(rows.Err(), rows.Close()); err != nil {
		return err
	}
	rows, err = tx.QueryContext(ctx, "SELECT id,scope,holder,start,until,selection,transfer,verification FROM window ORDER BY id")
	if err != nil {
		return err
	}
	for rows.Next() {
		var w stateWindow
		if err = rows.Scan(&w.ID, &w.Scope, &w.Holder, &w.From, &w.Until, &w.Selection, &w.Transfer, &w.Verification); err != nil {
			rows.Close()
			return err
		}
		result.Windows = append(result.Windows, w)
	}
	if err = errors.Join(rows.Err(), rows.Close()); err != nil {
		return err
	}
	if *id > 0 {
		rows, err = tx.QueryContext(ctx, "SELECT scope,window,holder,kind,reason,phase,first_seen,last_seen FROM issue_detail WHERE name_id=? ORDER BY scope,window,holder,kind", *id)
		if err != nil {
			return err
		}
		for rows.Next() {
			i := stateIssue{NameID: *id}
			if err = rows.Scan(&i.Scope, &i.Window, &i.Holder, &i.Kind, &i.Reason, &i.Phase, &i.FirstSeen, &i.LastSeen); err != nil {
				rows.Close()
				return err
			}
			result.Issues = append(result.Issues, i)
		}
		if err = errors.Join(rows.Err(), rows.Close()); err != nil {
			return err
		}
	}
	if *hashes {
		result.Digests, err = stateRelationDigests(tx)
		if err != nil {
			return err
		}
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	return json.NewEncoder(out).Encode(result)
}
