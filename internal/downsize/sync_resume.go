package downsize

import (
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
)

// Values include defaults so upgrading the binary cannot silently change scope.
// Credentials are never saved: only the signing secret's path is recorded.
type savedSyncConfig struct {
	RingIdentity string            `json:"ring_identity"`
	CopyState    string            `json:"copy_state,omitempty"`
	Version      int               `json:"version"`
	Flags        map[string]string `json:"flags"`
	Explicit     map[string]bool   `json:"explicit"`
}

func (c *syncCommand) restoreConfig(flags *flag.FlagSet, resume string) error {
	if resume == "" {
		return nil
	}
	c.resuming = true
	if info, err := os.Stat(resume); err != nil {
		return fmt.Errorf("--resume state directory: %w", err)
	} else if !info.IsDir() {
		return errors.New("--resume requires a state directory (for a legacy copy journal, use its .sync directory)")
	}
	data, err := os.ReadFile(filepath.Join(resume, "config.json"))
	if errors.Is(err, os.ErrNotExist) {
		if c.copy.to == "" || len(c.copy.froms) == 0 {
			return errors.New("legacy state has no saved config: provide the original --source, --to and remaining flags once with --resume; configuration will be saved")
		}
		if c.explicit["state"] && c.copy.state != resume && c.copy.state+".sync" != resume {
			return errors.New("--state conflicts with --resume")
		}
		if c.copy.state == "" || c.copy.state == resume {
			c.copy.state = resume
		}
		return nil
	}
	if err != nil {
		return fmt.Errorf("resume config: %w", err)
	}
	var saved savedSyncConfig
	if err := json.Unmarshal(data, &saved); err != nil {
		return fmt.Errorf("resume config: %w", err)
	}
	if saved.Version != 1 || saved.Flags == nil {
		return errors.New("unsupported or incomplete resume config")
	}
	overrides := map[string]string{}
	for name := range c.explicit {
		switch name {
		case "resume":
		case "streams", "rate":
			overrides[name] = flags.Lookup(name).Value.String()
		default:
			return fmt.Errorf("--resume uses saved configuration: --%s cannot change scope or configuration; only --streams and --rate may override it (use a new --state for a different run)", name)
		}
	}
	// Repeated flags append, so clear their CLI/default values before restoring.
	c.copy.froms, c.sync.windows = nil, nil
	names := make([]string, 0, len(saved.Flags))
	for name := range saved.Flags {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		f := flags.Lookup(name)
		if f == nil {
			return fmt.Errorf("saved config contains unsupported flag --%s", name)
		}
		if err := f.Value.Set(saved.Flags[name]); err != nil {
			return fmt.Errorf("saved --%s: %w", name, err)
		}
	}
	c.explicit = saved.Explicit
	if c.explicit == nil {
		c.explicit = map[string]bool{}
	}
	for name, value := range overrides {
		if err := flags.Lookup(name).Value.Set(value); err != nil {
			return err
		}
		c.explicit[name] = true
	}
	if _, ok := overrides["streams"]; ok {
		c.live.copyStreams, c.sync.verifyStreams = 0, 0
	}
	c.copy.state = resume
	c.sync.ringIdentity = saved.RingIdentity
	if saved.CopyState != "" {
		c.copy.state = saved.CopyState
	}
	return nil
}

func (c *syncCommand) captureConfig(flags *flag.FlagSet) {
	c.config = savedSyncConfig{RingIdentity: c.sync.ring, Version: 1, Flags: map[string]string{}, Explicit: map[string]bool{}}
	if c.sync.ringIdentity != "" {
		c.config.RingIdentity = c.sync.ringIdentity
	}
	if c.legacyJournal {
		c.config.CopyState = c.copy.state
	}
	flags.VisitAll(func(f *flag.Flag) {
		switch f.Name {
		case "resume", "from":
			return
		}
		c.config.Flags[f.Name] = f.Value.String()
	})
	for name, explicit := range c.explicit {
		if name != "resume" {
			c.config.Explicit[name] = explicit
		}
	}
	// source/to are normalized by parseSyncCommand; --from shares source storage.
	c.config.Flags["source"] = (*originList)(&c.copy.froms).String()
}

func (c *syncCommand) saveConfig() error {
	if !c.resuming {
		if _, err := os.Stat(filepath.Join(c.sync.state, "config.json")); err == nil {
			return errors.New("state already has saved configuration; use --resume <state-dir>")
		} else if !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	if err := c.validateResumeJournal(); err != nil {
		return err
	}
	if err := os.MkdirAll(c.sync.state, 0700); err != nil {
		return err
	}
	// Keep local input files beside the journal. --resume survives a different
	// working directory and removal of the original ring/name/block descriptor.
	for _, name := range []string{"ring", "names", "block"} {
		path := c.config.Flags[name]
		if path == "" {
			continue
		}
		input, err := os.Open(path)
		if err != nil {
			return fmt.Errorf("save --%s: %w", name, err)
		}
		dest, err := filepath.Abs(filepath.Join(c.sync.state, "config-"+name))
		if err != nil {
			input.Close()
			return err
		}
		absoluteInput, err := filepath.Abs(path)
		if err != nil {
			input.Close()
			return err
		}
		if absoluteInput != dest {
			err = atomicSyncConfigFrom(dest, input)
		}
		input.Close()
		if err != nil {
			return err
		}
		c.config.Flags[name] = dest
		switch name {
		case "ring":
			c.sync.ringIdentity = c.config.RingIdentity
			c.sync.ring, c.copy.ring = dest, dest
		case "names":
			c.sync.namesFile, c.copy.names = dest, dest
		}
	}
	if c.config.CopyState != "" {
		path, err := filepath.Abs(c.config.CopyState)
		if err != nil {
			return err
		}
		c.config.CopyState = path
	}
	secret, err := filepath.Abs(c.jwtSecret)
	if err != nil {
		return err
	}
	c.config.Flags["jwt-secret"] = secret
	data, err := json.MarshalIndent(c.config, "", "  ")
	if err != nil {
		return err
	}
	return atomicSyncConfigFile(filepath.Join(c.sync.state, "config.json"), append(data, '\n'))
}

func atomicSyncConfigFile(path string, data []byte) error {
	return atomicSyncConfigFrom(path, bytes.NewReader(data))
}

func atomicSyncConfigFrom(path string, data io.Reader) error {
	f, err := os.CreateTemp(filepath.Dir(path), ".sync-config-*")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	if _, err = io.Copy(f, data); err == nil {
		err = f.Sync()
	}
	closeErr := f.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	if err := os.Rename(f.Name(), path); err != nil {
		return err
	}
	dir, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}

// Check legacy argv against its durable scope before adopting it as config.
func (c *syncCommand) validateResumeJournal() error {
	path := filepath.Join(c.sync.state, "sync.jsonl")
	file, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	defer file.Close()
	var header syncRow
	if err := json.NewDecoder(file).Decode(&header); err != nil {
		return fmt.Errorf("read legacy journal header: %w", err)
	}
	identity, err := syncIdentity(c.sync)
	if err != nil {
		return err
	}
	if header.Kind != "header" || header.Identity != identity {
		return errors.New("resume scope differs from durable journal; provide the original sources, target, windows and ring (or use a new --state)")
	}
	return nil
}
