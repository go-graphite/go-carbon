package v2backend

import (
	"fmt"
	"github.com/go-graphite/go-carbon/internal/partlog"
)

type receiptEncoder interface {
	Encode([]partlog.DictionaryFrame) ([]byte, error)
	Close()
}

func validJournalCodec(name string) error {
	switch name {
	case "", "pdx1", "pdc3", "pdc4":
		return nil
	}
	return fmt.Errorf("unknown v2 journal codec %q", name)
}
func newReceiptEncoder(name string, catalog *partlog.NameCatalog) receiptEncoder {
	if name == "pdc4" {
		return &durableCatalogEncoder{partlog.CatalogEncoder{Catalog: catalog, SeriesClocks: true, SparseClocks: true}}
	}
	if name == "pdc3" {
		return &partlog.PredictorV3Encoder{SeriesClocks: true}
	}
	return &partlog.DictionaryDeltaEncoder{}
}
func (d *receiptDecoder) close() { d.dictionary.Close(); d.predictor.Close(); d.catalog.Close() }

// Definitions reach durable storage before referencing journal records.
type durableCatalogEncoder struct{ partlog.CatalogEncoder }

func (e *durableCatalogEncoder) Encode(fs []partlog.DictionaryFrame) ([]byte, error) {
	return e.EncodeDurable(fs)
}
