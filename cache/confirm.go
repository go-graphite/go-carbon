package cache

import "github.com/lomik/go-carbon/points"

type notConfirmed struct {
	data      map[string][]*points.Points
	queryChan chan *Query
}

func newNotConfirmed() *notConfirmed {
	return &notConfirmed{
		data:      make(map[string][]*points.Points),
		queryChan: make(chan *Query, 16),
	}
}

func (m *notConfirmed) add(p *points.Points) {
	values, exists := m.data[p.Metric]
	if !exists {
		values = make([]*points.Points, 0)
	}
	m.data[p.Metric] = append(values, p)
}

func (m *notConfirmed) confirm(p *points.Points) {
	values, exists := m.data[p.Metric]
	if !exists {
		return
	}

	if len(values) == 1 {
		if values[0] == p {
			delete(m.data, p.Metric)
		}
		return
	}

	if values[0] == p {
		m.data[p.Metric] = values[1:]
		return
	}

	for i, v := range values {
		if v == p {
			m.data[p.Metric] = append(values[:i], values[i+1:]...)
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

func (m *notConfirmed) worker(in, out, confirm chan *points.Points, exit chan bool) {
	var p, c *points.Points
	var sendTo, recvFrom chan *points.Points
	var q *Query

	for {
		if p == nil {
			recvFrom = in
			sendTo = nil
		} else {
			recvFrom = nil
			sendTo = out
		}

		select {
		case p = <-recvFrom:
			m.add(p)
		case sendTo <- p:
			p = nil
		case c = <-confirm:
			m.confirm(c)
		case q = <-m.queryChan:
			m.handleQuery(q)
		case <-exit:
			return
		}
	}
}
