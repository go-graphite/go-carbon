package udp

import "time"

type incompleteRecord struct {
	deadline time.Time
	data     []byte
}

// incompleteStorage store incomplete lines
type incompleteStorage struct {
	Records   map[string]*incompleteRecord
	Expires   time.Duration
	NextPurge time.Time
	MaxSize   int
}

func newIncompleteStorage() *incompleteStorage {
	return &incompleteStorage{
		Records:   make(map[string]*incompleteRecord, 0),
		Expires:   5 * time.Second,
		MaxSize:   10000,
		NextPurge: time.Now().Add(time.Second),
	}
}

func (storage *incompleteStorage) store(addr string, data []byte) {
	storage.Records[addr] = &incompleteRecord{
		deadline: time.Now().Add(storage.Expires),
		data:     data,
	}
	storage.checkAndClear()
}

func (storage *incompleteStorage) pop(addr string) []byte {
	if record, ok := storage.Records[addr]; ok {
		delete(storage.Records, addr)
		if record.deadline.Before(time.Now()) {
			return nil
		}
		return record.data
	}
	return nil
}

func (storage *incompleteStorage) purge() {
	now := time.Now()
	for key, record := range storage.Records {
		if record.deadline.Before(now) {
			delete(storage.Records, key)
		}
	}
	storage.NextPurge = time.Now().Add(time.Second)
}

func (storage *incompleteStorage) checkAndClear() {
	if len(storage.Records) < storage.MaxSize {
		return
	}
	if storage.NextPurge.After(time.Now()) {
		return
	}
	storage.purge()
}
