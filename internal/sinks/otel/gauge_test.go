package otel

import (
	"testing"
	"time"

	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/mercereau/hydrolix-metrics-go/internal/sinks"
)

func bufAdd(b *gaugeBuffer, name string, ts time.Time, value float64, tags sinks.Tags) {
	b.add(name, "line", attrsFromTags(tags), ts, value)
}

// Re-polling the same sliding window re-emits the same (series, timestamp)
// with the same or corrected value: the buffer must keep one point per
// (series, timestamp), last write winning.
func TestGaugeBufferDedupesSameSeriesAndTimestamp(t *testing.T) {
	b := newGaugeBuffer()
	ts := time.Unix(1_788_372_780, 0)
	tags := sinks.Tags{"app": "x", "level": "INFO"}

	bufAdd(b, "cluster_log_lines", ts, 87, tags)
	bufAdd(b, "cluster_log_lines", ts, 91, tags) // late data corrected the bucket

	ms := b.drain()
	if len(ms) != 1 {
		t.Fatalf("expected 1 metric, got %d", len(ms))
	}
	g, ok := ms[0].Data.(metricdata.Gauge[float64])
	if !ok {
		t.Fatalf("expected Gauge data, got %T", ms[0].Data)
	}
	if len(g.DataPoints) != 1 {
		t.Fatalf("expected 1 datapoint after dedupe, got %d", len(g.DataPoints))
	}
	if g.DataPoints[0].Value != 91 {
		t.Fatalf("last write must win: got %v, want 91", g.DataPoints[0].Value)
	}
}

// Different minute buckets of one series are distinct points, each carrying
// its own event timestamp and its raw value - never a delta.
func TestGaugeBufferKeepsRawValuesPerBucket(t *testing.T) {
	b := newGaugeBuffer()
	tags := sinks.Tags{"app": "x"}
	t1 := time.Unix(1000, 0)
	t2 := time.Unix(1060, 0)

	bufAdd(b, "m", t1, 87, tags)
	bufAdd(b, "m", t2, 74, tags) // a quieter minute: value drops

	ms := b.drain()
	g := ms[0].Data.(metricdata.Gauge[float64])
	if len(g.DataPoints) != 2 {
		t.Fatalf("expected 2 datapoints, got %d", len(g.DataPoints))
	}
	seen := map[int64]float64{}
	for _, dp := range g.DataPoints {
		seen[dp.Time.Unix()] = dp.Value
	}
	if seen[1000] != 87 || seen[1060] != 74 {
		t.Fatalf("raw values must survive per bucket, got %v", seen)
	}
}

// Distinct metric names drain into distinct metricdata entries, and draining
// empties the buffer.
func TestGaugeBufferGroupsByNameAndDrains(t *testing.T) {
	b := newGaugeBuffer()
	ts := time.Unix(1000, 0)
	bufAdd(b, "a", ts, 1, nil)
	bufAdd(b, "b", ts, 2, nil)

	ms := b.drain()
	if len(ms) != 2 {
		t.Fatalf("expected 2 metrics, got %d", len(ms))
	}
	if rest := b.drain(); len(rest) != 0 {
		t.Fatalf("drain must empty the buffer, got %d metrics", len(rest))
	}
}

// Gauge() must land in the buffer with the event timestamp and untouched
// value, with dimension attributes only - no event_time_* attributes, and no
// delta arithmetic against previous observations.
func TestGaugeGoesToBufferWithEventTimeAndRawValue(t *testing.T) {
	s := NewOTelSink(OTelOpts{Endpoint: "127.0.0.1:1", Insecure: true})
	ts := int64(1_788_372_780)
	scoped := s.WithTimestamp(ts)

	scoped.Gauge("cluster.log_lines", "line", 87, sinks.Tags{"app": "x"})
	scoped.Gauge("cluster.log_lines", "line", 74, sinks.Tags{"app": "x", "level": "INFO"})

	ms := s.c.gauges.drain()
	if len(ms) != 1 {
		t.Fatalf("expected 1 metric, got %d", len(ms))
	}
	g := ms[0].Data.(metricdata.Gauge[float64])
	if len(g.DataPoints) != 2 {
		t.Fatalf("expected 2 datapoints (distinct attr sets), got %d", len(g.DataPoints))
	}
	for _, dp := range g.DataPoints {
		if dp.Time.Unix() != ts {
			t.Errorf("datapoint time = %v, want event time %d", dp.Time.Unix(), ts)
		}
		if dp.Value != 87 && dp.Value != 74 {
			t.Errorf("value %v is neither raw input - delta arithmetic detected", dp.Value)
		}
		for _, kv := range dp.Attributes.ToSlice() {
			if k := string(kv.Key); k == "event_time_unix" || k == "event_time_ms" {
				t.Errorf("event time must be the datapoint timestamp, not attribute %s", k)
			}
		}
	}
}

// Without WithTimestamp (self-metrics), the observation time is used.
func TestGaugeWithoutTimestampUsesNow(t *testing.T) {
	s := NewOTelSink(OTelOpts{Endpoint: "127.0.0.1:1", Insecure: true})
	before := time.Now().Add(-time.Second)

	s.Gauge("self.metric", "row", 5, nil)

	ms := s.c.gauges.drain()
	g := ms[0].Data.(metricdata.Gauge[float64])
	if len(g.DataPoints) != 1 {
		t.Fatalf("expected 1 datapoint, got %d", len(g.DataPoints))
	}
	if dp := g.DataPoints[0]; dp.Time.Before(before) {
		t.Fatalf("expected ~now for missing event time, got %v", dp.Time)
	}
}
