package downsize

import (
	"context"
	"errors"
	"net/http"

	"github.com/go-graphite/go-carbon/internal/chunkstore"
)

// Native snapshots omit names with cached points. Merely reopening their pins
// cannot make cold queued points persist. The ordinary read export already
// flushes those points through the source persister. Use it only for the saved
// unresolved frontier, then let fresh native selection and window copy provide
// all coverage and target-wins proofs. Refresh only names still unresolved after
// native selection, in bounded batches. The server flushes the whole name list
// before attempting export; 404/413 remain non-proofs. Records are discarded.
func (b *syncLiveBackend) prepareWindowSelection(ctx context.Context, source string, names []string) error {
	defer b.expireWindowPins()
	for start := 0; start < len(names); start += 500 {
		subset := names[start:min(start+500, len(names))]
		if err := b.wait(ctx, "window selection flush", source, func() error {
			var records []chunkstore.TransferMetric
			_, err := b.m.requestOnce(ctx, source, "/v1/transfer/export?wire=2&store_format=12&max_bytes=65536", subset, &records)
			// Absence and an oversized history are not selection proofs. The
			// native scan below still decides whether this name can be retired.
			var api *apiError
			if errors.As(err, &api) && (api.Status == http.StatusNotFound || api.Status == http.StatusRequestEntityTooLarge) {
				return nil
			}
			return err
		}); err != nil {
			return err
		}
	}
	return nil
}
