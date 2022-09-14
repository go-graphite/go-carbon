package tags

import (
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sync/atomic"
	"time"

	"github.com/lomik/zapwriter"
	"go.uber.org/zap"

	"github.com/go-graphite/go-carbon/helper"
)

type Options struct {
	LocalPath           string
	TagDB               string
	TagDBTimeout        time.Duration
	TagDBChunkSize      int
	TagDBUpdateInterval uint64
}

type Tags struct {
	q             *Queue
	qErr          error // queue initialization error
	logger        *zap.Logger
	options       *Options
	updateCounter uint64
}

func New(options *Options) *Tags {
	var send func([]string) error

	t := &Tags{
		logger:  zapwriter.Logger("tags"),
		options: options,
	}

	u, urlErr := url.Parse(options.TagDB)
	if urlErr != nil {
		send = func([]string) error {
			time.Sleep(time.Second)
			t.logger.Error("bad tag url", zap.String("url", options.TagDB), zap.Error(urlErr))
			return urlErr
		}
	} else {
		u.Path = "/tags/tagMultiSeries"
		s := u.String()

		send = func(paths []string) error {
			client := &http.Client{Timeout: options.TagDBTimeout}

			resp, err := client.PostForm(s, url.Values{"path": paths})
			if err != nil {
				t.logger.Error("failed to post tags", zap.Error(err))
				return err
			}
			if resp.StatusCode != http.StatusOK {
				t.logger.Error("failed to post tags", zap.Int("status-code", resp.StatusCode))
				return fmt.Errorf("bad status code: %d", resp.StatusCode)
			}

			io.ReadAll(resp.Body)
			return nil
		}
	}

	t.q, t.qErr = NewQueue(options.LocalPath, send, options.TagDBChunkSize)
	if options.TagDBUpdateInterval < 1 {
		options.TagDBUpdateInterval = 1
	}

	return t
}

func (t *Tags) Stop() {
	t.q.Stop()
}

func (t *Tags) Add(value string, now bool) {
	if t.q == nil {
		t.logger.Error("queue database not initialized", zap.Error(t.qErr))
		return
	}
	if now || (atomic.AddUint64(&t.updateCounter, 1)%t.options.TagDBUpdateInterval == 0) {
		t.q.Add(value)
	}
}

// Collect metrics
func (t *Tags) Stat(send helper.StatCallback) {
	helper.SendAndSubstractUint32("queuePutErrors", &t.q.stat.putErrors, send)
	helper.SendAndSubstractUint32("queuePutCount", &t.q.stat.putCount, send)
	helper.SendAndSubstractUint32("queueDeleteErrors", &t.q.stat.deleteErrors, send)
	helper.SendAndSubstractUint32("queueDeleteCount", &t.q.stat.deleteCount, send)
	helper.SendAndSubstractUint32("tagdbSendFail", &t.q.stat.sendFail, send)
	helper.SendAndSubstractUint32("tagdbSendSuccess", &t.q.stat.sendSuccess, send)

	send("queueLag", t.q.Lag().Seconds())
}
