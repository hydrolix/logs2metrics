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

// memExporter records every Gauge datapoint it is asked to export and the
// metric names carried by each Export call, counts its Export calls, and
// fails them with err when set.
type memExporter struct {
	mu      sync.Mutex
	points  []metricdata.DataPoint[float64]
	calls   [][]string
	exports int // every Export call, empty ones included
	err     error
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
	m.exports++
	var names []string
	for _, sm := range rm.ScopeMetrics {
		for _, md := range sm.Metrics {
			names = append(names, md.Name)
			if g, ok := md.Data.(metricdata.Gauge[float64]); ok {
				m.points = append(m.points, g.DataPoints...)
			}
		}
	}
	if len(names) > 0 {
		m.calls = append(m.calls, names)
	}
	return m.err
}

// exportCalls returns the metric names of each non-empty Export call.
func (m *memExporter) exportCalls() [][]string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([][]string(nil), m.calls...)
}

func (m *memExporter) exportCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.exports
}

func (m *memExporter) failWith(err error) {
	m.mu.Lock()
	m.err = err
	m.mu.Unlock()
}

// blockingExporter never completes an export on its own, like a collector
// that accepts the connection but never answers; it returns only when the
// caller's context ends or release is closed.
type blockingExporter struct {
	memExporter
	release chan struct{}
}

func (b *blockingExporter) Export(ctx context.Context, _ *metricdata.ResourceMetrics) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-b.release:
		return nil
	}
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

// A collector that never answers must not hold up shutdown past the export
// timeout: the final flush is bounded, so the process exits within the
// orchestrator's grace period instead of being killed mid-flush.
func TestStopIsBoundedWhenExportBlocks(t *testing.T) {
	blk := &blockingExporter{release: make(chan struct{})}
	defer close(blk.release)
	orig := newExporter
	newExporter = func(OTelOpts, sdkmetric.TemporalitySelector) (sdkmetric.Exporter, error) { return blk, nil }
	t.Cleanup(func() { newExporter = orig })

	s := NewOTelSink(OTelOpts{ExportInterval: time.Hour, ExportTimeout: 100 * time.Millisecond})
	s.WithTimestamp(1_788_372_780).Gauge("cluster.log_lines", "line", 95, nil)

	stopped := make(chan struct{})
	go func() { s.Stop(); close(stopped) }()
	select {
	case <-stopped:
	case <-time.After(2 * time.Second):
		t.Fatal("Stop still blocked after 2s with a 100ms export timeout")
	}
}

// Gauges ride the reader's own export: one request per interval carries both
// SDK instruments and gauges, over a single exporter.
func TestGaugesAndCountersShareOneExport(t *testing.T) {
	mem := useMemExporter(t)
	created := 0
	withMem := newExporter
	newExporter = func(o OTelOpts, ts sdkmetric.TemporalitySelector) (sdkmetric.Exporter, error) {
		created++
		return withMem(o, ts)
	}
	s := NewOTelSink(OTelOpts{ExportInterval: time.Hour})

	s.WithTimestamp(1_788_372_780).Gauge("cluster.log_lines", "line", 95, nil)
	s.Inc("hydrolix.collector.poll", "total", 1, nil)
	s.Stop()

	if created != 1 {
		t.Errorf("created %d OTLP exporters, want 1", created)
	}
	calls := mem.exportCalls()
	if len(calls) != 1 {
		t.Fatalf("want one export carrying everything, got %d: %v", len(calls), calls)
	}
	has := map[string]bool{}
	for _, n := range calls[0] {
		has[n] = true
	}
	if !has["cluster_log_lines"] || !has["hydrolix_collector_poll"] {
		t.Fatalf("export carried %v, want both the gauge and the counter", calls[0])
	}
}

// Splunk Observability keeps one point per (series, timestamp) and silently
// drops a point older than the series' latest. A backfill export carrying
// several minutes of one series must therefore send them oldest first, or
// every minute before the newest is lost.
func TestGaugePointsExportedOldestFirst(t *testing.T) {
	mem := useMemExporter(t)
	s := NewOTelSink(OTelOpts{ExportInterval: time.Hour})
	start := time.Unix(1_788_372_780, 0).UTC()

	const minutes = 20
	for _, i := range []int{7, 3, 19, 0, 12, 5, 16, 1, 9, 14, 2, 18, 6, 11, 4, 17, 8, 13, 10, 15} {
		ts := start.Add(time.Duration(i) * time.Minute).Unix()
		s.WithTimestamp(ts).Gauge("cluster.log_lines", "line", float64(i), sinks.Tags{"level": "INFO"})
		s.WithTimestamp(ts).Gauge("cluster.log_lines", "line", float64(i), sinks.Tags{"level": "WARN"})
	}
	s.Stop()

	last := map[string]time.Time{}
	n := 0
	for _, p := range mem.gaugePoints() {
		lvl, _ := p.Attributes.Value("level")
		series := lvl.AsString()
		if prev, ok := last[series]; ok && !p.Time.After(prev) {
			t.Fatalf("series level=%s: point at %v exported after %v", series, p.Time, prev)
		}
		last[series] = p.Time
		n++
	}
	if n != 2*minutes {
		t.Fatalf("exported %d points, want %d", n, 2*minutes)
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
