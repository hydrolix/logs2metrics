// Package otel provides an OpenTelemetry-backed MetricSink.
// It exports metrics to an OTLP collector using the Go OTel SDK.
package otel

import (
	"context"
	"log/slog"
	"sort"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/sdk/instrumentation"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.opentelemetry.io/otel/sdk/resource"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"

	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"

	"github.com/mercereau/hydrolix-metrics-go/internal/sinks"
)

// -------- Options & construction --------

type OTelOpts struct {
	// OTLP endpoint: "localhost:4317" for gRPC, or "localhost:4318" / "http://localhost:4318" for HTTP.
	Endpoint string

	// "grpc" (default) or "http"
	Protocol string

	// Export interval for the periodic reader (default 10s).
	ExportInterval time.Duration

	// Resource info
	ServiceName    string
	ServiceVersion string
	DeploymentEnv  string            // "prod", "stage", etc.
	ResourceAttrs  map[string]string // extra resource attrs

	// If true, use insecure transport for gRPC (no TLS).
	Insecure bool

	// Temporality preference: "delta" (default) or "cumulative".
	Temporality string

	// SelfSink receives internal health metrics (gauge evictions).
	// Must NOT be this OTel sink itself — use Prometheus or Nop (default).
	SelfSink sinks.MetricSink
}

func NewOTelSink(opts OTelOpts) *OTelScoped {
	if opts.ExportInterval <= 0 {
		opts.ExportInterval = 10 * time.Second
	}
	if opts.Temporality == "" {
		opts.Temporality = "delta"
	}
	self := opts.SelfSink
	if self == nil {
		self = sinks.NewNop(nil)
	}
	core := &otelCore{
		opts:       opts,
		counters:   map[iKey]metric.Float64Counter{},
		histograms: map[iKey]metric.Float64Histogram{},
		gauges:     newGaugeBuffer(),
		self:       self.WithTags(sinks.Tags{"sink": "otel"}),
	}
	return &OTelScoped{c: core}
}

// -------- Sink implementation (scoped handle) --------

type OTelScoped struct {
	c    *otelCore
	base sinks.Tags
	ts   *time.Time // optional override (stored as attributes)
}

func (s *OTelScoped) Name() string { return "otel" }
func (s *OTelScoped) Start()       { s.c.start() }
func (s *OTelScoped) Stop()        { _ = s.c.stop(context.Background()) }

func (s *OTelScoped) WithTags(t sinks.Tags) sinks.MetricSink {
	return &OTelScoped{c: s.c, base: mergeTags(s.base, t), ts: s.ts}
}

func (s *OTelScoped) WithTimestamp(ts int64) sinks.MetricSink {
	t := toTime(ts) // supports sec or msec epoch
	return &OTelScoped{c: s.c, base: s.base, ts: &t}
}

func (s *OTelScoped) Inc(name, unit string, value float64, tags sinks.Tags) {
	if value <= 0 {
		return // counters are monotonic
	}
	s.c.ensureStarted()
	meter := s.c.meter

	key := iKey{name: sanitizeName(name), unit: sanitizeUnit(unit), kind: "counter"}
	inst := s.c.getCounter(meter, key)

	at := attrsFromTags(mergeTags(s.base, tags))
	if s.ts != nil {
		at = appendEventTimeAttrs(at, *s.ts)
	}
	inst.Add(context.Background(), value, metric.WithAttributes(at...))
}

func (s *OTelScoped) Rate(name, unit string, value float64, tags sinks.Tags) {
	// Record as counter; compute rate in backend/queries.
	s.Inc(name, unit, value, tags)
}

func (s *OTelScoped) Gauge(name, unit string, value float64, tags sinks.Tags) {
	// Gauges are exported as OTLP Gauge datapoints carrying the query's value
	// and the event timestamp, so overlapping-window re-polls overwrite rather
	// than accumulate. See gaugeBuffer for why this bypasses the SDK Meter.
	s.c.ensureStarted()

	t := time.Now()
	if s.ts != nil {
		t = *s.ts
	}
	s.c.gauges.add(sanitizeName(name), sanitizeUnit(unit), attrsFromTags(mergeTags(s.base, tags)), t, value)
}

func (s *OTelScoped) Timing(name string, d time.Duration, tags sinks.Tags) {
	s.c.ensureStarted()
	meter := s.c.meter

	key := iKey{name: sanitizeName(name), unit: "s", kind: "hist"}
	inst := s.c.getHistogram(meter, key)

	at := attrsFromTags(mergeTags(s.base, tags))
	if s.ts != nil {
		at = appendEventTimeAttrs(at, *s.ts)
	}
	inst.Record(context.Background(), d.Seconds(), metric.WithAttributes(at...))
}

// -------- Core: exporter, provider, instrument caches --------

type otelCore struct {
	opts OTelOpts

	mu      sync.Mutex
	started bool

	exp    sdkmetric.Exporter
	reader *sdkmetric.PeriodicReader
	mp     *sdkmetric.MeterProvider
	meter  metric.Meter

	// instruments cached by (name, unit, kind)
	insMu      sync.RWMutex
	counters   map[iKey]metric.Float64Counter
	histograms map[iKey]metric.Float64Histogram

	// gauge export: hand-built datapoints flushed on the export interval
	// through a dedicated exporter (the PeriodicReader owns exp exclusively).
	gauges    *gaugeBuffer
	gaugeExp  sdkmetric.Exporter
	gaugeRes  *resource.Resource
	gaugeStop chan struct{}
	gaugeDone chan struct{}

	self sinks.MetricSink // sink for self-monitoring metrics
}

func (c *otelCore) start() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.started {
		return
	}

	// Resource
	var attrs []attribute.KeyValue
	if c.opts.ServiceName != "" {
		attrs = append(attrs, semconv.ServiceNameKey.String(c.opts.ServiceName))
	}
	if c.opts.ServiceVersion != "" {
		attrs = append(attrs, semconv.ServiceVersionKey.String(c.opts.ServiceVersion))
	}
	if c.opts.DeploymentEnv != "" {
		attrs = append(attrs, semconv.DeploymentEnvironmentKey.String(c.opts.DeploymentEnv))
	}
	for k, v := range c.opts.ResourceAttrs {
		attrs = append(attrs, attribute.String(k, v))
	}
	res, _ := resource.New(context.Background(),
		resource.WithAttributes(attrs...),
		resource.WithFromEnv(), // allow OTEL_RESOURCE_ATTRIBUTES env too
	)

	// Exporter (temporality set here, not on PeriodicReader)
	temporal := strings.ToLower(c.opts.Temporality)
	var ts sdkmetric.TemporalitySelector
	if temporal == "cumulative" {
		ts = CumulativeTemporalitySelector
	} else {
		ts = DeltaTemporalitySelector
	}

	newExporter := func() (sdkmetric.Exporter, error) {
		switch strings.ToLower(c.opts.Protocol) {
		case "http", "http/protobuf", "http_protobuf":
			clientOpts := []otlpmetrichttp.Option{}
			if c.opts.Endpoint != "" {
				clientOpts = append(clientOpts, otlpmetrichttp.WithEndpoint(c.opts.Endpoint))
			}
			clientOpts = append(clientOpts, otlpmetrichttp.WithTemporalitySelector(ts))
			return otlpmetrichttp.New(context.Background(), clientOpts...)
		default: // gRPC
			clientOpts := []otlpmetricgrpc.Option{}
			if c.opts.Endpoint != "" {
				clientOpts = append(clientOpts, otlpmetricgrpc.WithEndpoint(c.opts.Endpoint))
			}
			if c.opts.Insecure {
				clientOpts = append(clientOpts, otlpmetricgrpc.WithInsecure())
			}
			clientOpts = append(clientOpts, otlpmetricgrpc.WithTemporalitySelector(ts))
			return otlpmetricgrpc.New(context.Background(), clientOpts...)
		}
	}

	// Build both exporters before constructing anything stateful, so a
	// failure here leaves no reader goroutine or global provider behind.
	exp, err := newExporter()
	if err != nil {
		// In your project, handle/log the error as needed.
		return
	}
	gaugeExp, err := newExporter()
	if err != nil {
		_ = exp.Shutdown(context.Background())
		return
	}

	reader := sdkmetric.NewPeriodicReader(
		exp,
		sdkmetric.WithInterval(c.opts.ExportInterval),
	)
	mp := sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(reader),
		sdkmetric.WithResource(res),
	)
	otel.SetMeterProvider(mp)
	meter := mp.Meter("cdn-metrics")

	c.exp = exp
	c.reader = reader
	c.mp = mp
	c.meter = meter
	c.gaugeExp = gaugeExp
	c.gaugeRes = res
	c.gaugeStop = make(chan struct{})
	c.gaugeDone = make(chan struct{})
	c.started = true
	go c.runGaugeFlusher()
}

// runGaugeFlusher exports buffered gauge datapoints on the export interval,
// with a final flush at shutdown.
func (c *otelCore) runGaugeFlusher() {
	defer close(c.gaugeDone)
	ticker := time.NewTicker(c.opts.ExportInterval)
	defer ticker.Stop()
	for {
		select {
		case <-c.gaugeStop:
			c.flushGauges(context.Background())
			return
		case <-ticker.C:
			c.flushGauges(context.Background())
		}
	}
}

func (c *otelCore) flushGauges(ctx context.Context) {
	metrics := c.gauges.drain()
	if len(metrics) == 0 {
		return
	}
	rm := &metricdata.ResourceMetrics{
		Resource: c.gaugeRes,
		ScopeMetrics: []metricdata.ScopeMetrics{{
			Scope:   instrumentation.Scope{Name: "cdn-metrics"},
			Metrics: metrics,
		}},
	}
	// No retry: the sliding window re-polls the same buckets within one
	// interval, so a failed flush re-buffers itself; only the final flush at
	// shutdown is lossy on error, and that is reported below.
	if err := c.gaugeExp.Export(ctx, rm); err != nil {
		c.self.Inc("hydrolix.sink.send_failures", "total", 1, nil)
		slog.Error("Failed to export gauge metrics", "error", err)
	}
}

func (c *otelCore) ensureStarted() { c.start() }

func (c *otelCore) stop(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.started {
		return nil
	}
	close(c.gaugeStop)
	<-c.gaugeDone // final gauge flush has run
	gaugeErr := c.gaugeExp.Shutdown(ctx)
	err := c.mp.Shutdown(ctx) // flushes and closes the reader's exporter
	c.started = false
	c.exp = nil
	c.reader = nil
	c.mp = nil
	c.meter = nil // interface zero
	c.gaugeExp = nil
	if err == nil {
		err = gaugeErr
	}
	return err
}

// instrument key
type iKey struct {
	name string
	unit string
	kind string // "counter", "updown", "hist"
}

func (c *otelCore) getCounter(m metric.Meter, k iKey) metric.Float64Counter {
	c.insMu.RLock()
	inst, ok := c.counters[k]
	c.insMu.RUnlock()
	if ok {
		return inst
	}
	c.insMu.Lock()
	defer c.insMu.Unlock()
	if inst, ok = c.counters[k]; ok {
		return inst
	}
	i, _ := m.Float64Counter(k.name, metric.WithUnit(k.unit), metric.WithDescription(k.name))
	c.counters[k] = i
	return i
}

func (c *otelCore) getHistogram(m metric.Meter, k iKey) metric.Float64Histogram {
	c.insMu.RLock()
	inst, ok := c.histograms[k]
	c.insMu.RUnlock()
	if ok {
		return inst
	}
	c.insMu.Lock()
	defer c.insMu.Unlock()
	if inst, ok = c.histograms[k]; ok {
		return inst
	}
	i, _ := m.Float64Histogram(k.name, metric.WithUnit(k.unit), metric.WithDescription(k.name))
	c.histograms[k] = i
	return i
}

// -------- helpers --------

func attrsFromTags(t sinks.Tags) []attribute.KeyValue {
	if len(t) == 0 {
		return nil
	}
	keys := make([]string, 0, len(t))
	for k := range t {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	kv := make([]attribute.KeyValue, 0, len(keys))
	for _, k := range keys {
		kv = append(kv, attribute.String(sanitizeAttrKey(k), t[k]))
	}
	return kv
}

func appendEventTimeAttrs(in []attribute.KeyValue, t time.Time) []attribute.KeyValue {
	return append(in,
		attribute.Int64("event_time_unix", t.Unix()),
		attribute.Int64("event_time_ms", t.UnixMilli()),
	)
}

func mergeTags(a, b sinks.Tags) sinks.Tags {
	if len(a) == 0 {
		out := make(sinks.Tags, len(b))
		for k, v := range b {
			out[k] = v
		}
		return out
	}
	out := make(sinks.Tags, len(a)+len(b))
	for k, v := range a {
		out[k] = v
	}
	for k, v := range b {
		out[k] = v
	}
	return out
}

func sanitizeName(s string) string {
	s = strings.ToLower(strings.TrimSpace(s))
	s = strings.ReplaceAll(s, " ", "_")
	s = strings.ReplaceAll(s, ".", "_")
	return s
}
func sanitizeUnit(u string) string {
	return strings.TrimSpace(u) // OTel accepts UCUM-like units; keep as provided
}
func sanitizeAttrKey(k string) string {
	k = strings.TrimSpace(strings.ToLower(k))
	if k == "" {
		return "attr"
	}
	return k
}

func toTime(ts int64) time.Time {
	// seconds vs milliseconds
	if ts < 1_000_000_000_000 {
		return time.Unix(ts, 0).UTC()
	}
	sec := ts / 1000
	ms := ts % 1000
	return time.Unix(sec, ms*int64(time.Millisecond)).UTC()
}

// -------- Temporality selectors (helpers for older SDKs) --------

func DeltaTemporalitySelector(kind sdkmetric.InstrumentKind) metricdata.Temporality {
	return metricdata.DeltaTemporality
}

func CumulativeTemporalitySelector(kind sdkmetric.InstrumentKind) metricdata.Temporality {
	return metricdata.CumulativeTemporality
}
