package datadog

import (
	"errors"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/DataDog/datadog-api-client-go/v2/api/datadogV2"

	metrics "github.com/mercereau/hydrolix-metrics-go/internal/sinks"
)

// tagSink is a self-sink that records increments with their merged tags.
type tagSink struct {
	metrics.Nop
	mu   *sync.Mutex
	incs *[]tagInc
	base metrics.Tags
}

type tagInc struct {
	name string
	tags metrics.Tags
}

func newTagSink() tagSink { return tagSink{mu: &sync.Mutex{}, incs: &[]tagInc{}} }

func (s tagSink) WithTags(t metrics.Tags) metrics.MetricSink {
	return tagSink{mu: s.mu, incs: s.incs, base: metrics.MergeTags(s.base, t)}
}

func (s tagSink) Inc(name, _ string, _ float64, tags metrics.Tags) {
	s.mu.Lock()
	*s.incs = append(*s.incs, tagInc{name: name, tags: metrics.MergeTags(s.base, tags)})
	s.mu.Unlock()
}

// count returns how many increments of name carry every tag in want.
func (s tagSink) count(name string, want metrics.Tags) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, in := range *s.incs {
		if in.name != name {
			continue
		}
		match := true
		for k, v := range want {
			if in.tags[k] != v {
				match = false
				break
			}
		}
		if match {
			n++
		}
	}
	return n
}

// fakeSubmit replaces the Datadog API call for the test.
func fakeSubmit(t *testing.T, status int, err error) {
	t.Helper()
	orig := submitMetrics
	submitMetrics = func(datadogV2.MetricPayload) (datadogV2.IntakePayloadAccepted, *http.Response, error) {
		return datadogV2.IntakePayloadAccepted{}, &http.Response{StatusCode: status}, err
	}
	t.Cleanup(func() { submitMetrics = orig })
}

func newExportTestSink(self metrics.MetricSink, retries int) *Sink {
	return NewSink(DatadogOpts{
		Namespace: "t", Subsystem: "t",
		MaxRetries:     retries,
		InitialBackoff: time.Millisecond,
		SelfSink:       self,
	})
}

// hydrolix.sink.export is the export-outcome metric shared by the push
// sinks (the OTel sink reports it too), so one alert covers both: a
// delivered payload counts once as success, alongside payloads_sent.
func TestSendIsCountedAsExport(t *testing.T) {
	fakeSubmit(t, http.StatusAccepted, nil)
	self := newTagSink()
	dd := newExportTestSink(self, 3)

	if err := dd.sendToDatadog(datadogV2.MetricPayload{}); err != nil {
		t.Fatalf("send failed: %v", err)
	}

	if ok := self.count("hydrolix.sink.export", metrics.Tags{"sink": "datadog", "status": "success"}); ok != 1 {
		t.Fatalf("a delivered payload must count once as export status=success, got %d", ok)
	}
	if bad := self.count("hydrolix.sink.export", metrics.Tags{"status": "error"}); bad != 0 {
		t.Fatalf("nothing failed, but export status=error = %d", bad)
	}
	if sent := self.count("hydrolix.sink.payloads_sent", nil); sent != 1 {
		t.Fatalf("payloads_sent must stay, got %d", sent)
	}
}

// A payload that fails every retry counts once as export status=error - per
// payload, not per attempt - alongside the existing send_failures.
func TestFailedSendIsCountedAsExportError(t *testing.T) {
	fakeSubmit(t, http.StatusServiceUnavailable, errors.New("unavailable"))
	self := newTagSink()
	dd := newExportTestSink(self, 3)

	if err := dd.sendToDatadog(datadogV2.MetricPayload{}); err == nil {
		t.Fatal("send must fail when every attempt fails")
	}

	if bad := self.count("hydrolix.sink.export", metrics.Tags{"sink": "datadog", "status": "error"}); bad != 1 {
		t.Fatalf("a payload failing all 3 attempts must count once as export status=error, got %d", bad)
	}
	if ok := self.count("hydrolix.sink.export", metrics.Tags{"status": "success"}); ok != 0 {
		t.Fatalf("nothing was delivered, but export status=success = %d", ok)
	}
	if sf := self.count("hydrolix.sink.send_failures", nil); sf != 1 {
		t.Fatalf("send_failures must stay, got %d", sf)
	}
}
