package v2daemon

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/go-graphite/go-carbon/internal/v2core"
)

// A legacy request was published only after PrepareMove had verified every
// immutable file and synced its read index. Import that certificate directly;
// owner catalogs reconcile any activation that preceded the legacy done marker.
func (a *MoveAdmin) importLegacy(id string) error {
	dir := filepath.Join(a.root, id)
	b, e := os.ReadFile(filepath.Join(dir, "request.json"))
	if e != nil {
		return e
	}
	var request MoveRequest
	if e = json.Unmarshal(b, &request); e != nil {
		return e
	}
	if len(request.Parts) == 0 {
		return errors.New("empty legacy move")
	}
	files := make([]MoveFileStatus, 0, len(request.Parts))
	for _, part := range request.Parts {
		if e = part.Descriptor.Validate(); e != nil {
			return e
		}
		name := fmt.Sprintf("%x", part.Descriptor.SHA256)
		st, e := os.Stat(filepath.Join(dir, name+".sst"))
		if e != nil {
			return e
		}
		if st.Size() != int64(part.Descriptor.Bytes) {
			return errors.New("legacy move file size changed")
		}
		files = append(files, MoveFileStatus{Name: name, Size: st.Size(), Checksum: name, State: "pending"})
	}
	if e = a.requireMoveVersion(); e != nil {
		return e
	}
	m := &moveRecord{Version: MoveRecordVersion, MoveStatus: MoveStatus{ID: id, Phase: "verified", Parts: files, PartsTotal: len(files)}, Request: request, Started: time.Now().UnixNano()}
	for range a.store.Core.DBs() {
		m.Owners = append(m.Owners, MoveOwnerStatus{State: "verified"})
	}
	for i := range m.Parts {
		m.BytesTotal += m.Parts[i].Size
		for range m.Owners {
			m.Parts[i].Owners = append(m.Parts[i].Owners, "verified")
		}
	}
	m.Request.Parts = append([]v2core.MovePart(nil), request.Parts...)
	for i := range m.Request.Parts {
		m.Request.Parts[i].Path = filepath.Join(dir, files[i].Name+".sst")
		m.Parts[i].State = "verified"
		for j := range m.Parts[i].Owners {
			m.Parts[i].Owners[j] = "verified"
		}
	}
	m.Phase = "verified"
	m.PartsDone = m.PartsTotal
	m.BytesDone = m.BytesTotal
	for i := range m.Owners {
		m.Owners[i].State = "verified"
	}
	if _, e = os.Stat(filepath.Join(dir, "done")); e == nil {
		m.Done = true
		m.Phase = "done"
		m.Finished = time.Now().UnixNano()
		m.Parts = nil
		m.Request.Parts = nil
		for i := range m.Owners {
			m.Owners[i].State = "active"
			m.Owners[i].PartsDone = m.PartsTotal
			m.Owners[i].PartsTotal = m.PartsTotal
			m.Owners[i].BytesDone = m.BytesTotal
			m.Owners[i].BytesTotal = m.BytesTotal
		}
	} else if !errors.Is(e, os.ErrNotExist) {
		return e
	}
	return a.save(m)
}
