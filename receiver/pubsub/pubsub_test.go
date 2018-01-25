package pubsub

import (
	"context"
	"fmt"
	"testing"
	"time"

	"cloud.google.com/go/pubsub"
	"cloud.google.com/go/pubsub/pstest"
	"github.com/lomik/go-carbon/points"
	"github.com/stretchr/testify/assert"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
)

var (
	testProject = "test-proj"
	testTopic   = "test-topic"
	testSub     = "test-sub"
)

func newTestClient() (*pstest.Server, *pubsub.Topic, *pubsub.Client, error) {
	ctx := context.Background()
	srv := pstest.NewServer()
	srv.SetStreamTimeout(500 * time.Millisecond)
	conn, err := grpc.Dial(srv.Addr, grpc.WithInsecure())
	if err != nil {
		return nil, nil, nil, err
	}
	client, err := pubsub.NewClient(ctx, testProject, option.WithGRPCConn(conn))
	if err != nil {
		return nil, nil, nil, err
	}
	topic, err := client.CreateTopic(ctx, testTopic)
	if err != nil {
		return nil, nil, nil, err
	}
	_, err = client.CreateSubscription(ctx, testSub, pubsub.SubscriptionConfig{Topic: topic, AckDeadline: 10 * time.Second})
	if err != nil {
		return nil, nil, nil, err
	}
	return srv, topic, client, nil
}

func Test_handleMessage(t *testing.T) {
	_, _, client, err := newTestClient()
	if err != nil {
		t.Fatal(err)
	}

	received := make([]*points.Points, 0)
	storeFn := func(p *points.Points) {
		received = append(received, p)
	}
	opts := &Options{
		Project:      testProject,
		Subscription: testSub,
	}
	r, err := newPubSub(client, "pubsub", opts, storeFn)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Stop()

	tests := []struct {
		Desc     string
		Data     string
		Attrs    map[string]string
		Expected []*points.Points
		Error    bool
	}{
		{
			Desc: "linemode, valid body, multiple points",
			Data: "hello.world 42.15 1422698155\nmetric.name -72.11 1422698155\n",
			Expected: []*points.Points{
				points.OnePoint("hello.world", 42.15, 1422698155),
				points.OnePoint("metric.name", -72.11, 1422698155),
			},
		},
		{
			Desc:     "linemode, invalid body",
			Data:     "hello.world 42.15 1422698155\nmetric.nam",
			Error:    true,
			Expected: []*points.Points{},
		},
		{
			Desc: "pickle, invalid body",
			Data: "hello.world 42.15 1422698155\nmetric.name -72.11 1422698155\n",
			Attrs: map[string]string{
				"content-type": "application/python-pickle",
			},
			Error:    true,
			Expected: []*points.Points{},
		},
		{
			Desc: "pickle, valid body, multiple points",
			Data: "(lp0\n(S'param1'\np1\n(I1423931224\nF60.2\ntp2\n(I1423931284\nI42\ntp3\ntp4\na(S'param2'\np5\n(I1423931224\nI-15\ntp6\ntp7\na.",
			Attrs: map[string]string{
				"content-type": "application/python-pickle",
			},
			Expected: []*points.Points{
				points.OnePoint("param1", 60.2, 1423931224).Add(42, 1423931284),
				points.OnePoint("param2", -15, 1423931224),
			},
		},
		{
			Desc: "protobuf, valid body, multiple points",
			Data: "\n*\n\x06param1\x12\x0f\x08\xd8\xee\xfd\xa6\x05\x11\x9a\x99\x99\x99\x99\x19N@\x12\x0f\x08\x94\xef\xfd\xa6\x05\x11\x00\x00\x00\x00\x00\x00E@\n\x19\n\x06param2\x12\x0f\x08\xd8\xee\xfd\xa6\x05\x11\x00\x00\x00\x00\x00\x00.\xc0",
			Attrs: map[string]string{
				"content-type": "application/protobuf",
			},
			Expected: []*points.Points{
				points.OnePoint("param1", 60.2, 1423931224).Add(42, 1423931284),
				points.OnePoint("param2", -15, 1423931224),
			},
		},
	}

	for _, tc := range tests {
		received = make([]*points.Points, 0)
		r.metricsReceived = 0
		r.errors = 0
		r.handleMessage(&pubsub.Message{
			ID:         tc.Desc,
			Data:       []byte(tc.Data),
			Attributes: tc.Attrs,
		})
		if !tc.Error {
			assert.Equal(t, uint32(0), r.errors, fmt.Sprintf("test: %s", tc.Desc))
			assert.Equal(t, tc.Expected, received, fmt.Sprintf("test: %s", tc.Desc))
		} else {
			assert.Equal(t, uint32(1), r.errors, fmt.Sprintf("test: %s", tc.Desc))
			assert.Equal(t, tc.Expected, received, fmt.Sprintf("test: %s", tc.Desc))
		}
	}
}
