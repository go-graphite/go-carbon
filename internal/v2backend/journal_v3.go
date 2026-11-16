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
	case "", "pdx1", "pdc3":
		return nil
	}
	return fmt.Errorf("unknown v2 journal codec %q", name)
}
func newReceiptEncoder(name string) receiptEncoder {
	if name == "pdc3" {
		return &partlog.PredictorV3Encoder{SeriesClocks: true}
	}
	return &partlog.DictionaryDeltaEncoder{}
}
func (d *receiptDecoder) close() { d.dictionary.Close(); d.predictor.Close() }
