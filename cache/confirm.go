package cache

import (
	"fmt"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/lomik/go-carbon/points"
)

type notConfirmed struct {
	data           map[string][]*points.Points
	queryChan      chan *Query
	metricInterval time.Duration
	graphPrefix    string
	in             chan *points.Points
	out            chan *points.Points
	confirmed      chan *points.Points
	cacheIn        chan *points.Points
	size           int
}

func (m *notConfirmed) add(p *points.Points) {
	values, exists := m.data[p.Metric]
	if !exists {
		values = make([]*points.Points, 0)
	}
	m.data[p.Metric] = append(values, p)
	m.size++
}

func (m *notConfirmed) confirm(p *points.Points) {
	values, exists := m.data[p.Metric]
	if !exists {
		return
	}

	if len(values) == 1 {
		if values[0] == p {
			delete(m.data, p.Metric)
			m.size--
		}
		return
	}

	if values[0] == p {
		m.data[p.Metric] = values[1:]
		m.size--
		return
	}

	for i, v := range values {
		if v == p {
			m.data[p.Metric] = append(values[:i], values[i+1:]...)
			m.size--
			return
		}
	}
}

func (m *notConfirmed) handleQuery(query *Query) {
	values, exists := m.data[query.Metric]
	if exists {
		query.InFlightData = values
	}

	close(query.Wait)
}

// stat send internal statistics of cache
func (m *notConfirmed) stat(metric string, value float64) {
	key := fmt.Sprintf("%scache.%s", m.graphPrefix, metric)
	p := points.NowPoint(key, value)
	m.cacheIn <- p
}

func (m *notConfirmed) doCheckpoint() {
	m.stat("notConfirmedSize", float64(m.size))

	logrus.WithFields(logrus.Fields{
		"notConfirmedSize": m.size,
	}).Info("[cache] notConfirmed.doCheckpoint()")
}

func (m *notConfirmed) worker(exit chan bool) {
	var p, c *points.Points
	var sendTo, recvFrom chan *points.Points
	var q *Query

	ticker := time.NewTicker(m.metricInterval)
	defer ticker.Stop()

	for {
		if p == nil {
			recvFrom = m.in
			sendTo = nil
		} else {
			recvFrom = nil
			sendTo = m.out
		}

		select {
		case p = <-recvFrom:
			m.add(p)
		case sendTo <- p:
			p = nil
		case c = <-m.confirmed:
			m.confirm(c)
		case q = <-m.queryChan:
			m.handleQuery(q)
		case <-ticker.C:
			m.doCheckpoint()
		case <-exit:
			return
		}
	}
}
