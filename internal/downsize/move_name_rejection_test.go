package downsize

import (
	"errors"
	"fmt"
	"testing"
)

func TestPermanentMetricNameRejection(t *testing.T) {
	for _, tc := range []struct {
		err  error
		want bool
	}{
		{&apiError{500, "metric name too long\n"}, true},
		{fmt.Errorf("wrapped: %w", &apiError{500, "http://target:4242 /v1/transfer/import: HTTP 500 metric name too long\n"}), true},
		{&apiError{500, "metric name too long: temporary index failure"}, false},
		{&apiError{500, "temporary failure"}, false},
		{errors.New("metric name too long"), false},
	} {
		_, got := maximumSpeedErrorKind(transferIntegrityError(tc.err))
		if got != tc.want {
			t.Errorf("%v: integrity=%v want=%v", tc.err, got, tc.want)
		}
	}
}
