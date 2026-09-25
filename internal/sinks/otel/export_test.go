package otel

import (
	"context"
	"sync"
	"testing"
	"time"

	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/mercereau/hydrolix-metrics-go/internal/sinks"
)

// memExporter records every Gauge datapoint it is asked to export, counts
// its Export calls, and fails them with err when set.
type memExporter struct {
	mu         sync.Mutex
	points     []metricdata.DataPoint[float64]
	calls      int // every Export call
	gaugeCalls int // Export calls carrying Gauge data (the gauge flusher)
	err        error
}

func (m *memExporter) Temporality(sdkmetric.InstrumentKind) metricdata.Temporality {
	return metricdata.DeltaTemporality
}

func (m *memExporter) Aggregation(k sdkmetric.InstrumentKind) sdkmetric.Aggregation {
	return sdkmetric.DefaultAggregationSelector(k)
}

func (m *memExporter) Export(_ context.Context, rm *metricdata.ResourceMetrics) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls++
	hasGauge := false
	for _, sm := range rm.ScopeMetrics {
		for _, md := range sm.Metrics {
			if g, ok := md.Data.(metricdata.Gauge[float64]); ok {
				m.points = append(m.points, g.DataPoints...)
				hasGauge = true
			}
		}
	}
	if hasGauge {
		m.gaugeCalls++
	}
	return m.err
}

func (m *memExporter) callCounts() (calls, gaugeCalls int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.calls, m.gaugeCalls
}

func (m *memExporter) failWith(err error) {
	m.mu.Lock()
	m.err = err
	m.mu.Unlock()
}

func (m *memExporter) ForceFlush(context.Context) error { return nil }
func (m *memExporter) Shutdown(context.Context) error   { return nil }

func (m *memExporter) gaugePoints() []metricdata.DataPoint[float64] {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]metricdata.DataPoint[float64](nil), m.points...)
}

// useMemExporter makes the sink export through an in-memory exporter.
func useMemExporter(t *testing.T) *memExporter {
	t.Helper()
	mem := &memExporter{}
	orig := newExporter
	newExporter = func(OTelOpts, sdkmetric.TemporalitySelector) (sdkmetric.Exporter, error) { return mem, nil }
	t.Cleanup(func() { newExporter = orig })
	return mem
}

// Gauges still buffered when the collector shuts down must be exported by
// Stop, not dropped: the final poll's values would otherwise never arrive.
func TestStopExportsBufferedGauges(t *testing.T) {
	mem := useMemExporter(t)
	s := NewOTelSink(OTelOpts{ExportInterval: time.Hour}) // no periodic flush during the test
	bucket := time.Unix(1_788_372_780, 0).UTC()

	s.WithTimestamp(bucket.Unix()).Gauge("cluster.log_lines", "line", 95, sinks.Tags{"level": "INFO"})
	if got := len(mem.gaugePoints()); got != 0 {
		t.Fatalf("nothing should be exported before Stop, got %d points", got)
	}
	s.Stop()

	pts := mem.gaugePoints()
	if len(pts) != 1 {
		t.Fatalf("Stop should export the buffered gauge, got %d points", len(pts))
	}
	if pts[0].Value != 95 || !pts[0].Time.Equal(bucket) {
		t.Fatalf("exported point = %v at %v, want 95 at %v", pts[0].Value, pts[0].Time, bucket)
	}
}

// While running, buffered gauges go out on the export interval.
func TestGaugesExportedOnInterval(t *testing.T) {
	mem := useMemExporter(t)
	s := NewOTelSink(OTelOpts{ExportInterval: 20 * time.Millisecond})
	defer s.Stop()

	s.WithTimestamp(1_788_372_780).Gauge("cluster.log_lines", "line", 42, nil)

	deadline := time.Now().Add(2 * time.Second)
	for len(mem.gaugePoints()) == 0 {
		if time.Now().After(deadline) {
			t.Fatal("gauge was not exported within 2s at a 20ms export interval")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if pts := mem.gaugePoints(); len(pts) != 1 || pts[0].Value != 42 {
		t.Fatalf("want exactly one exported point with value 42, got %+v", pts)
	}
}
