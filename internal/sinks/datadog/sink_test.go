package datadog

import (
	"encoding/json"
	"net/http"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	ddv2 "github.com/DataDog/datadog-api-client-go/v2/api/datadogV2"
	"github.com/mercereau/hydrolix-metrics-go/internal/sinks"
)

func TestCreateMetricAndFlattenTags(t *testing.T) {
	tags := sinks.Tags{"env": "prod", "zone": "eu"}
	ts := int64(1234567890)
	m := CreateMetric(ddv2.METRICINTAKETYPE_GAUGE, "my.metric", "ms", ts, 3.14, tags)

	if m.Metric != "my.metric" {
		t.Fatalf("unexpected metric name: %q", m.Metric)
	}
	if m.Unit == nil || *m.Unit != "ms" {
		t.Fatalf("unexpected unit: %#v", m.Unit)
	}
	if len(m.Points) != 1 {
		t.Fatalf("expected 1 point, got %d", len(m.Points))
	}
	if m.Points[0].Timestamp == nil || *m.Points[0].Timestamp != ts {
		t.Fatalf("timestamp mismatch: %#v", m.Points[0].Timestamp)
	}
	if m.Points[0].Value == nil || *m.Points[0].Value != 3.14 {
		t.Fatalf("value mismatch: %#v", m.Points[0].Value)
	}

	flat := flattenTags(tags)
	// order not guaranteed; check both keys present
	joined := strings.Join(flat, ",")
	if !strings.Contains(joined, "env:prod") || !strings.Contains(joined, "zone:eu") {
		t.Fatalf("flattened tags missing entries: %v", flat)
	}
}

func TestSeriesListString_JSON(t *testing.T) {
	tags := sinks.Tags{"k": "v"}
	s := SeriesList{}
	// create a series and append
	m := CreateMetric(ddv2.METRICINTAKETYPE_COUNT, "s.test", "c", 1, 2.0, tags)
	s = append(s, m)

	out := s.String()
	// Should be valid JSON containing the metric name
	var parsed []ddv2.MetricSeries
	if err := json.Unmarshal([]byte(out), &parsed); err != nil {
		t.Fatalf("result not valid JSON: %v; out=%s", err, out)
	}
	if len(parsed) != 1 || parsed[0].Metric != "s.test" {
		t.Fatalf("unexpected parsed content: %#v", parsed)
	}
}

// A batch handed to the sender must not change afterwards: the collector keeps
// filling its next batch while the sender is still working on the last one.
// The fake sender holds the first payload until the collector has buffered
// the next batch, then records what it was given. Covers both flush paths:
// a full batch and the flush timer. Run with -race.
func TestFlushedBatchIsNotOverwrittenByTheNextOne(t *testing.T) {
	cases := []struct {
		name      string
		batchSize int
		interval  time.Duration
	}{
		{"batch full", 2, time.Hour},
		{"flush timer", 1 << 20, 20 * time.Millisecond},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var mu sync.Mutex
			var sent []float64
			held, release := make(chan struct{}), make(chan struct{})
			var first sync.Once
			orig := submitMetrics
			submitMetrics = func(p ddv2.MetricPayload) (ddv2.IntakePayloadAccepted, *http.Response, error) {
				first.Do(func() { close(held); <-release })
				mu.Lock()
				for _, s := range p.Series {
					sent = append(sent, *s.Points[0].Value)
				}
				mu.Unlock()
				return ddv2.IntakePayloadAccepted{}, &http.Response{StatusCode: http.StatusAccepted}, nil
			}
			t.Cleanup(func() { submitMetrics = orig })

			s := NewSink(DatadogOpts{
				Namespace: "t", Subsystem: "t",
				FlushInterval: tc.interval, QueueSize: 8, BatchSize: tc.batchSize,
				MaxRetries: 1, Concurrency: 1,
			})
			s.Start()
			s.Gauge("m", "unit", 0, nil)
			s.Gauge("m", "unit", 1, nil) // first batch: flushed to the sender
			<-held
			s.Gauge("m", "unit", 2, nil)
			s.Gauge("m", "unit", 3, nil) // second batch, written while the first is held
			time.Sleep(100 * time.Millisecond)
			close(release)
			s.Stop()

			mu.Lock()
			defer mu.Unlock()
			if want := []float64{0, 1, 2, 3}; !slices.Equal(sent, want) {
				t.Fatalf("sent values %v, want %v", sent, want)
			}
		})
	}
}

// Per-call tags must survive the whole pipeline (buffer, batch, send), not
// just CreateMetric: the poller's reason tag on error polls rides on them.
func TestPerCallTagsReachThePayload(t *testing.T) {
	var mu sync.Mutex
	var sent []ddv2.MetricSeries
	orig := submitMetrics
	submitMetrics = func(p ddv2.MetricPayload) (ddv2.IntakePayloadAccepted, *http.Response, error) {
		mu.Lock()
		sent = append(sent, p.Series...)
		mu.Unlock()
		return ddv2.IntakePayloadAccepted{}, &http.Response{StatusCode: http.StatusAccepted}, nil
	}
	t.Cleanup(func() { submitMetrics = orig })

	s := NewSink(DatadogOpts{
		Namespace: "t", Subsystem: "t",
		FlushInterval: time.Hour, QueueSize: 8, BatchSize: 1 << 20,
		MaxRetries: 1, Concurrency: 1,
	})
	s.Start()
	s.WithTags(sinks.Tags{"query": "q1"}).Inc("hydrolix.collector.poll", "total", 1,
		sinks.Tags{"status": "error", "reason": "auth"})
	s.Stop()

	mu.Lock()
	defer mu.Unlock()
	if len(sent) != 1 {
		t.Fatalf("want 1 series sent, got %d", len(sent))
	}
	got := map[string]bool{}
	for _, tag := range sent[0].Tags {
		got[tag] = true
	}
	for _, want := range []string{"query:q1", "status:error", "reason:auth"} {
		if !got[want] {
			t.Errorf("sent tags %v, missing %q", sent[0].Tags, want)
		}
	}
}
