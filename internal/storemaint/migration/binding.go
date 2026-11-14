// Package migration validates reboot-stable online-migration volume bindings.
// Only legacy V6 device identifiers may be rewritten, under Pebble's record lock.
package migration

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"
)

type root struct {
	Path   string `json:"path"`
	UUID   string `json:"uuid"`
	Inode  uint64 `json:"inode"`
	Device uint64 `json:"device"`
}
type proof struct {
	Version      int             `json:"version"`
	RecordedAt   float64         `json:"recorded_at"`
	LastRebindAt float64         `json:"last_rebind_at,omitempty"`
	Roots        map[string]root `json:"roots"`
}
type change struct {
	path      string
	old, next []byte
}
type binding struct {
	volume  func(string) (string, error)
	birth   func(string, root) (string, error)
	replace func(string, []byte) error
}

func defaults() binding { return binding{volume: volume, birth: birth, replace: atomic} }

var uuidPattern = regexp.MustCompile(`^[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$`)
var tokenPattern = regexp.MustCompile(`^[0-9a-f]{32}\n$`)
var fractionPattern = regexp.MustCompile(`\.([0-9]{9}) [+-][0-9]{4}$`)

func command(name string, args ...string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return exec.CommandContext(ctx, name, args...).Output()
}
func volume(path string) (string, error) {
	data, err := command("findmnt", "--json", "-T", path, "-o", "UUID,FSTYPE")
	if err != nil {
		return "", err
	}
	var rows struct {
		Filesystems []struct {
			UUID   string `json:"uuid"`
			FSType string `json:"fstype"`
		} `json:"filesystems"`
	}
	if err = json.Unmarshal(data, &rows); err != nil {
		return "", err
	}
	if len(rows.Filesystems) != 1 || rows.Filesystems[0].FSType != "xfs" || !uuidPattern.MatchString(rows.Filesystems[0].UUID) {
		return "", errors.New("automatic rebinding requires XFS with a filesystem UUID")
	}
	return rows.Filesystems[0].UUID, nil
}
func statRoot(path string) (root, error) {
	canonical, err := filepath.EvalSymlinks(path)
	if err != nil {
		return root{}, err
	}
	if !filepath.IsAbs(path) || filepath.Clean(path) != path || canonical != path {
		return root{}, errors.New("binding directories must be canonical without symlinks")
	}
	info, err := os.Lstat(path)
	if err != nil {
		return root{}, err
	}
	if !info.IsDir() {
		return root{}, errors.New("binding root is not a directory")
	}
	s, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return root{}, errors.New("directory identity unavailable")
	}
	return root{Path: path, Inode: s.Ino, Device: uint64(s.Dev)}, nil
}
func (b binding) identity(path string) (root, error) {
	r, err := statRoot(path)
	if err != nil {
		return r, err
	}
	r.UUID, err = b.volume(path)
	return r, err
}
func birth(path string, current root) (string, error) {
	data, err := command("stat", "--printf=%d\n%i\n%W\n%w\n", "--", path)
	if err != nil {
		return "", err
	}
	fields := strings.Split(strings.TrimSuffix(string(data), "\n"), "\n")
	if len(fields) != 4 {
		return "", errors.New("source directory birth time unavailable")
	}
	device, e1 := strconv.ParseUint(fields[0], 10, 64)
	inode, e2 := strconv.ParseUint(fields[1], 10, 64)
	seconds, e3 := strconv.ParseUint(fields[2], 10, 64)
	frac := fractionPattern.FindStringSubmatch(fields[3])
	after, err := statRoot(path)
	if e1 != nil || e2 != nil || e3 != nil || err != nil || seconds == 0 || len(frac) != 2 {
		return "", errors.New("source directory birth time unavailable")
	}
	if current.Device != device || current.Inode != inode || after.Device != device || after.Inode != inode {
		return "", errors.New("source changed while reading stable identity")
	}
	return fmt.Sprintf("birth:%d:%d:%s", inode, seconds, frac[1]), nil
}
func readRegular(path string) ([]byte, error) {
	fd, err := syscall.Open(path, syscall.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
	if err != nil {
		return nil, err
	}
	f := os.NewFile(uintptr(fd), path)
	defer f.Close()
	s, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if !s.Mode().IsRegular() || s.Size() > 16384 {
		return nil, fmt.Errorf("invalid binding file: %s", path)
	}
	data, err := io.ReadAll(io.LimitReader(f, 16385))
	if len(data) > 16384 {
		return nil, errors.New("binding file too large")
	}
	return data, err
}
func syncDir(path string) error {
	fd, err := syscall.Open(path, syscall.O_RDONLY|syscall.O_DIRECTORY|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return err
	}
	f := os.NewFile(uintptr(fd), path)
	defer f.Close()
	return f.Sync()
}
func atomic(path string, data []byte) error {
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() {
		return errors.New("binding target is not regular")
	}
	s := info.Sys().(*syscall.Stat_t)
	f, err := os.CreateTemp(filepath.Dir(path), ".volume-rebind-*")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	defer f.Close()
	if _, err = f.Write(data); err != nil {
		return err
	}
	if err = f.Chown(int(s.Uid), int(s.Gid)); err != nil {
		return err
	}
	if err = f.Chmod(info.Mode() & (os.ModePerm | os.ModeSetuid | os.ModeSetgid | os.ModeSticky)); err != nil {
		return err
	}
	if err = f.Sync(); err != nil {
		return err
	}
	if err = f.Close(); err != nil {
		return err
	}
	if err = os.Rename(f.Name(), path); err != nil {
		return err
	}
	return syncDir(filepath.Dir(path))
}
func markers(p proof) map[string]string {
	r := p.Roots
	paths := map[string]string{"source": filepath.Join(r["store"].Path, "INCREMENTAL-FILE-MIGRATION"), "guard": filepath.Join(r["source"].Path, ".go-carbon-incremental-source-root.keep")}
	if _, ok := r["archive"]; ok {
		paths["archive"] = filepath.Join(r["store"].Path, "INCREMENTAL-SOURCE-ARCHIVE.json")
	}
	return paths
}
func markerBytes(roots map[string]root) map[string][]byte {
	s := roots["source"]
	guard, _ := json.Marshal(map[string]any{"version": 1, "path": s.Path, "device": s.Device, "inode": s.Inode})
	values := map[string][]byte{"source": []byte(fmt.Sprintf("go-carbon-incremental-v1\n%s\n%d:%d\n", s.Path, s.Device, s.Inode)), "guard": guard}
	if a, ok := roots["archive"]; ok {
		values["archive"], _ = json.Marshal(map[string]any{"Version": 1, "Path": a.Path, "Device": a.Device, "Inode": a.Inode})
	}
	return values
}
func equalJSON(a, b []byte) bool {
	var x, y any
	dx := json.NewDecoder(bytes.NewReader(a))
	dx.UseNumber()
	dy := json.NewDecoder(bytes.NewReader(b))
	dy.UseNumber()
	return dx.Decode(&x) == nil && dy.Decode(&y) == nil && reflect.DeepEqual(x, y) && dx.Decode(new(any)) == io.EOF && dy.Decode(new(any)) == io.EOF
}
func exists(path string) bool { _, err := os.Lstat(path); return !errors.Is(err, os.ErrNotExist) }
func (b binding) current(p proof) (map[string]root, error) {
	if p.Version != 1 || len(p.Roots) < 2 || len(p.Roots) > 3 {
		return nil, errors.New("invalid volume proof")
	}
	if _, ok := p.Roots["source"]; !ok {
		return nil, errors.New("missing source root")
	}
	if _, ok := p.Roots["store"]; !ok {
		return nil, errors.New("missing store root")
	}
	if len(p.Roots) == 3 {
		if _, ok := p.Roots["archive"]; !ok {
			return nil, errors.New("invalid root")
		}
	}
	current := map[string]root{}
	for name, old := range p.Roots {
		now, err := b.identity(old.Path)
		if err != nil {
			return nil, err
		}
		if old.Path != now.Path || old.UUID != now.UUID || old.Inode != now.Inode {
			return nil, fmt.Errorf("filesystem or directory replaced: %s", name)
		}
		current[name] = now
	}
	return current, nil
}
func (b binding) validate(p proof, current map[string]root) ([]change, string, error) {
	store := p.Roots["store"].Path
	format, err := readRegular(filepath.Join(store, "CHUNKSTORE"))
	if err != nil {
		return nil, "", err
	}
	switch string(format) {
	case "go-carbon-chunks-v6\n", "go-carbon-chunks-v7\n", "go-carbon-chunks-v9\n", "go-carbon-chunks-v11\n", "go-carbon-chunks-v12\n":
	default:
		return nil, "", errors.New("supported continuation format required (V6/V7/V9/V11/V12)")
	}
	paths := markers(p)
	raw, err := readRegular(paths["source"])
	if err != nil {
		return nil, "", err
	}
	_, archive := p.Roots["archive"]
	hasArchive := exists(filepath.Join(store, "INCREMENTAL-SOURCE-ARCHIVE.json"))
	if strings.HasPrefix(string(raw), "go-carbon-incremental-v2\n") {
		if archive || hasArchive {
			return nil, "", errors.New("stable source validation requires no archive policy")
		}
		parts := strings.Split(string(raw), "\n")
		if len(parts) != 4 || parts[1] != p.Roots["source"].Path || parts[3] != "" {
			return nil, "", errors.New("unexpected stable source binding")
		}
		recorded := parts[2]
		source := current["source"]
		var wanted string
		if strings.HasPrefix(recorded, "token:") {
			token, err := readRegular(filepath.Join(source.Path, ".go-carbon-migration-source"))
			if err != nil {
				return nil, "", err
			}
			if !tokenPattern.Match(token) {
				return nil, "", errors.New("invalid source identity token")
			}
			wanted = "token:" + strings.TrimSuffix(string(token), "\n")
		} else if strings.HasPrefix(recorded, "birth:") {
			wanted, err = b.birth(source.Path, source)
			if err != nil {
				return nil, "", err
			}
		} else {
			return nil, "", errors.New("unsupported stable source identity")
		}
		if wanted != recorded {
			return nil, "", errors.New("unexpected stable source identity")
		}
		if exists(paths["guard"]) {
			guard, err := readRegular(paths["guard"])
			if err != nil {
				return nil, "", err
			}
			if !equalJSON(guard, markerBytes(current)["guard"]) {
				return nil, "", errors.New("unexpected source guard")
			}
		}
		return nil, wanted, nil
	}
	if string(format) != "go-carbon-chunks-v6\n" {
		return nil, "", errors.New("modern continuation format requires a stable V2 source binding")
	}
	if !archive && hasArchive {
		return nil, "", errors.New("unrecorded archive policy")
	}
	old, want := markerBytes(p.Roots), markerBytes(current)
	var changes []change
	keys := make([]string, 0, len(paths))
	for k := range paths {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, name := range keys {
		path := paths[name]
		raw, err := readRegular(path)
		if err != nil {
			return nil, "", err
		}
		equal := bytes.Equal
		if name != "source" {
			equal = equalJSON
		}
		if !equal(raw, old[name]) && !equal(raw, want[name]) {
			return nil, "", fmt.Errorf("unexpected directory binding: %s", path)
		}
		if !equal(raw, want[name]) {
			changes = append(changes, change{path: path, old: raw, next: append(want[name], '\n')})
			if name == "source" {
				changes[len(changes)-1].next = want[name]
			}
		}
	}
	return changes, "", nil
}
func writeNew(path string, data []byte) error {
	fd, err := syscall.Open(path, syscall.O_WRONLY|syscall.O_CREAT|syscall.O_EXCL|syscall.O_NOFOLLOW, 0600)
	if err != nil {
		return err
	}
	f := os.NewFile(uintptr(fd), path)
	defer f.Close()
	if _, err = f.Write(data); err != nil {
		return err
	}
	if err = f.Sync(); err != nil {
		return err
	}
	if err = f.Close(); err != nil {
		return err
	}
	return syncDir(filepath.Dir(path))
}
func (b binding) record(path, source, store, archive string) (proof, error) {
	p := proof{Version: 1, RecordedAt: float64(time.Now().UnixNano()) / 1e9, Roots: map[string]root{}}
	paths := map[string]string{"source": source, "store": store}
	if archive != "" {
		paths["archive"] = archive
	}
	uuid := ""
	for name, path := range paths {
		r, err := b.identity(path)
		if err != nil {
			return p, err
		}
		if uuid != "" && uuid != r.UUID {
			return p, errors.New("online migration roots must share the recorded filesystem")
		}
		uuid = r.UUID
		p.Roots[name] = r
	}
	for a, left := range p.Roots {
		for c, right := range p.Roots {
			if a != c && (left.Path == right.Path || strings.HasPrefix(right.Path, left.Path+string(os.PathSeparator))) {
				return p, errors.New("migration roots must be disjoint")
			}
		}
	}
	changes, _, err := b.validate(p, p.Roots)
	if err != nil {
		return p, err
	}
	if len(changes) != 0 {
		return p, errors.New("record requires already-valid bindings")
	}
	data, err := json.MarshalIndent(p, "", "  ")
	if err != nil {
		return p, err
	}
	return p, writeNew(path, append(data, '\n'))
}
func lockStore(path string) (*os.File, error) {
	fd, err := syscall.Open(filepath.Join(path, "LOCK"), syscall.O_RDWR|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return nil, err
	}
	f := os.NewFile(uintptr(fd), "LOCK")
	lock := syscall.Flock_t{Type: syscall.F_WRLCK, Whence: 0}
	if err = syscall.FcntlFlock(f.Fd(), syscall.F_SETLK, &lock); err != nil {
		f.Close()
		return nil, err
	}
	return f, nil
}
func (b binding) rebind(path string, inspect bool) (map[string]any, error) {
	data, err := readRegular(path)
	if err != nil {
		return nil, err
	}
	var p proof
	if err = json.Unmarshal(data, &p); err != nil {
		return nil, err
	}
	current, err := b.current(p)
	if err != nil {
		return nil, err
	}
	changes, stable, err := b.validate(p, current)
	if err != nil {
		return nil, err
	}
	result := map[string]any{"changed": false, "pending_changes": len(changes), "updated_markers": 0, "roots": current}
	if stable != "" {
		result["source_identity"] = stable
		return result, nil
	}
	if inspect {
		return result, nil
	}
	if reflect.DeepEqual(p.Roots, current) && len(changes) == 0 {
		return result, nil
	}
	lock, err := lockStore(p.Roots["store"].Path)
	if err != nil {
		return nil, err
	}
	defer lock.Close()
	current, err = b.current(p)
	if err != nil {
		return nil, err
	}
	changes, _, err = b.validate(p, current)
	if err != nil {
		return nil, err
	}
	if len(changes) > 0 {
		history, err := os.MkdirTemp(filepath.Dir(path), "volume-rebind-*")
		if err != nil {
			return nil, err
		}
		for i, c := range changes {
			if err = writeNew(filepath.Join(history, fmt.Sprintf("%d-%s.before", i, filepath.Base(c.path))), c.old); err != nil {
				return nil, err
			}
		}
		if err = syncDir(history); err != nil {
			return nil, err
		}
		if err = syncDir(filepath.Dir(history)); err != nil {
			return nil, err
		}
		for _, c := range changes {
			if err = b.replace(c.path, c.next); err != nil {
				return nil, err
			}
		}
	}
	if !reflect.DeepEqual(p.Roots, current) {
		p.Roots = current
		p.LastRebindAt = float64(time.Now().UnixNano()) / 1e9
		data, err = json.MarshalIndent(p, "", "  ")
		if err != nil {
			return nil, err
		}
		if err = b.replace(path, append(data, '\n')); err != nil {
			return nil, err
		}
	}
	result["changed"] = len(changes) > 0
	result["updated_markers"] = len(changes)
	result["pending_changes"] = 0
	result["roots"] = current
	return result, nil
}
func Main(args []string) error {
	if len(args) == 0 {
		return errors.New("usage: storectl maintain migration record|inspect|rebind -proof FILE [flags]")
	}
	mode := args[0]
	fs := flag.NewFlagSet("storectl maintain migration "+mode, flag.ContinueOnError)
	path := fs.String("proof", "", "recorded volume identity proof")
	source := fs.String("source", "", "canonical source directory (record only)")
	store := fs.String("store", "", "canonical store directory (record only)")
	archive := fs.String("archive", "", "canonical archive directory (record only)")
	if err := fs.Parse(args[1:]); err != nil {
		return err
	}
	if *path == "" || fs.NArg() != 0 {
		return errors.New("migration requires -proof")
	}
	b := defaults()
	var result any
	var err error
	switch mode {
	case "record":
		if *source == "" || *store == "" {
			return errors.New("record requires -source and -store")
		}
		result, err = b.record(*path, *source, *store, *archive)
	case "inspect", "rebind":
		if *source != "" || *store != "" || *archive != "" {
			return errors.New("inspect/rebind uses only the recorded proof")
		}
		result, err = b.rebind(*path, mode == "inspect")
	default:
		return fmt.Errorf("unknown migration action %q", mode)
	}
	if err != nil {
		return err
	}
	return json.NewEncoder(os.Stdout).Encode(result)
}
