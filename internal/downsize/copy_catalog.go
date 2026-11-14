package downsize

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"sync"
)

// Catalog readers share a durable paged log. Both the copier and its background
// counter replay it; only one reader fetches the next page. A cursor never skips
// pending imports: discovered names are replayed through the transfer journal.
type copyCatalog struct {
	mu         sync.Mutex
	failed     error
	file       *os.File
	path       string
	checkpoint catalogCheckpoint
	fetch      func(context.Context, string) ([]string, error)
	progress   func(catalogCheckpoint)
}
type catalogCheckpoint struct {
	After string `json:"after"`
	Bytes int64  `json:"bytes"`
	Names int64  `json:"names"`
	Done  bool   `json:"done"`
}

func openCopyCatalog(path string, fetch func(context.Context, string) ([]string, error), progress func(catalogCheckpoint)) (*copyCatalog, error) {
	cp := catalogCheckpoint{}
	data, err := os.ReadFile(path + ".json")
	if err == nil {
		if err = json.Unmarshal(data, &cp); err != nil {
			return nil, err
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	log, err := openDurableTSV(path+".names", cp.Bytes)
	if err != nil {
		return nil, err
	}
	if err = syncDirectory(filepath.Dir(path)); err != nil {
		log.file.Close()
		return nil, err
	}
	c := &copyCatalog{file: log.file, path: path, checkpoint: cp, fetch: fetch, progress: progress}
	if progress != nil {
		progress(cp)
	}
	return c, nil
}

func (c *copyCatalog) ensure(ctx context.Context, offset int64) (bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.failed != nil {
		return false, c.failed
	}
	if offset < c.checkpoint.Bytes {
		return true, nil
	}
	if c.checkpoint.Done {
		return false, nil
	}
	if err := ctx.Err(); err != nil {
		return false, err
	}
	page, err := c.fetch(ctx, c.checkpoint.After)
	if err != nil {
		return false, err
	}
	cp := c.checkpoint
	var data []byte
	for _, n := range page {
		if n <= cp.After {
			return false, fmt.Errorf("source catalog not byte-sorted at %q after %q", n, cp.After)
		}
		data = append(data, n...)
		data = append(data, '\n')
		cp.After = n
		cp.Names++
	}
	if _, err = c.file.Write(data); err != nil {
		c.failed = err
		return false, err
	}
	if err = c.file.Sync(); err != nil {
		c.failed = err
		return false, err
	}
	cp.Bytes += int64(len(data))
	cp.Done = len(page) == 0
	if err = atomicStateJSON(c.path+".json", cp); err != nil {
		c.failed = err
		return false, err
	}
	c.checkpoint = cp
	if c.progress != nil {
		c.progress(cp)
	}
	return !cp.Done, nil
}
func (c *copyCatalog) reader(ctx context.Context) (func() (string, error), func(), error) {
	f, err := os.Open(c.path + ".names")
	if err != nil {
		return nil, nil, err
	}
	r := bufio.NewReaderSize(f, 1<<20)
	var offset int64
	return func() (string, error) {
		if err := ctx.Err(); err != nil {
			return "", err
		}
		ok, err := c.ensure(ctx, offset)
		if err != nil {
			return "", err
		}
		if !ok {
			return "", io.EOF
		}
		line, err := r.ReadString('\n')
		if err != nil {
			return "", fmt.Errorf("catalog log at %d: %w", offset, err)
		}
		offset += int64(len(line))
		return line[:len(line)-1], nil
	}, func() { _ = f.Close() }, nil
}

func (c *planFreeCopier) catalogSource(ctx context.Context, from string) (func() (string, error), func(), error) {
	c.catalogMu.Lock()
	defer c.catalogMu.Unlock()
	if c.catalogs == nil {
		c.catalogs = map[string]*copyCatalog{}
	}
	catalog := c.catalogs[from]
	if catalog == nil {
		identity, err := copyIdentity(c.o)
		if err != nil {
			return nil, nil, err
		}
		sum := sha256.Sum256([]byte(identity + "\n" + from))
		path := c.o.state + ".catalog-" + hex.EncodeToString(sum[:8])
		catalog, err = openCopyCatalog(path, func(ctx context.Context, after string) ([]string, error) {
			query := url.Values{"after": {after}, "limit": {strconv.Itoa(copyListPage)}}
			if c.o.prefix != "" {
				query.Set("prefix", c.o.prefix)
			}
			var page []string
			err := c.retryTransient(ctx, from+"/v1/transfer/list", []string{from}, func() error {
				page = nil
				_, err := c.m.request(ctx, from, "/v1/transfer/list?"+query.Encode(), nil, &page)
				return err
			})
			if len(page) > copyListPage {
				return nil, errors.New("oversize source catalog page")
			}
			return page, err
		}, func(cp catalogCheckpoint) {
			c.catalogProgressMu.Lock()
			defer c.catalogProgressMu.Unlock()
			if c.catalogProgress == nil {
				c.catalogProgress = map[string]catalogCheckpoint{}
			}
			c.catalogProgress[from] = cp
		})
		if err != nil {
			return nil, nil, err
		}
		c.catalogs[from] = catalog
	}
	return catalog.reader(ctx)
}
func (c *planFreeCopier) closeCatalogs() {
	for _, cat := range c.catalogs {
		_ = cat.file.Close()
	}
}
func (c *planFreeCopier) progressSnapshot() progressSnapshot {
	p := progressSnapshot{Effective: c.o.policy, Done: c.stats.copied.Load() + int64(c.skippedCount()), Total: max(0, c.progress.total.Load()), Phase: "copy", Cursors: map[string]string{}, Copied: c.stats.copied.Load()}
	if c.journal != nil {
		c.journal.mu.Lock()
		p.Copied = c.journal.completedNames
		p.Verified = p.Copied
		c.journal.mu.Unlock()
	}
	c.catalogProgressMu.Lock()
	defer c.catalogProgressMu.Unlock()
	for source, cp := range c.catalogProgress {
		p.Cursors[source] = cp.After
		p.Discovered += cp.Names
		if !cp.Done {
			p.Phase = "discover-copy"
		}
	}
	if c.o.names != "" {
		p.Discovered = c.stats.discovered.Load()
	}
	return p
}
