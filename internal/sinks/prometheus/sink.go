// Package prometheus provides a Prometheus-backed MetricSink with an optional
// self-hosted /metrics HTTP server controlled via Start()/Stop().
package prometheus

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"regexp"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	prom "github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	dto "github.com/prometheus/client_model/go"

	"github.com/mercereau/hydrolix-metrics-go/internal/sinks"
)

// ---------- Core registry & vectors (shared across scopes) ----------
type promCore struct {
	reg  *prom.Registry
	opts PromOpts
	mu   sync.RWMutex

	slots map[string]*slot // one per metric name (see slot)

	// HTTP server state
	srvMu   sync.Mutex
	srv     *http.Server
	ln      net.Listener
	running bool
	srvWg   sync.WaitGroup

	healthPath    string
	healthHandler http.Handler
}

// PromScoped is the user-facing handle (implements MetricSink) and can also start/stop its own server.
type PromScoped struct {
	p    *promCore
	base sinks.Tags
}

// PromOpts controls registration, histogram buckets, and the optional HTTP server.
type PromOpts struct {
	Namespace            string
	Subsystem            string
	HistogramBuckets     []float64
	RegisterGoCollectors bool

	// Optional embedded HTTP server for /metrics (Start/Stop)
	ListenAddr      string        // default ":2112"
	MetricsPath     string        // default "/metrics"
	ReadTimeout     time.Duration // optional
	WriteTimeout    time.Duration // optional
	IdleTimeout     time.Duration // optional
	ShutdownTimeout time.Duration // default 5s
}

// NewSink returns a MetricSink that can also serve /metrics via Start().
func NewSink(opts PromOpts) *PromScoped {
	if len(opts.HistogramBuckets) == 0 {
		opts.HistogramBuckets = []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10}
	}
	core := &promCore{
		reg:   prom.NewRegistry(),
		opts:  opts,
		slots: map[string]*slot{},
	}
	if opts.RegisterGoCollectors {
		core.reg.MustRegister(collectors.NewGoCollector())
		core.reg.MustRegister(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
		// optional (adds myapp_build_info{version,revision} 1)
		// core.reg.MustRegister(collectors.NewBuildInfoCollector())
	}
	return &PromScoped{p: core, base: nil}
}

func (s *PromScoped) Name() string { return "prometheus" }

// Start launches the self-hosted HTTP server exposing /sinks.
// It returns immediately; errors are swallowed by design. Call Stop() to shutdown.
func (s *PromScoped) Start() {
	s.p.startHTTP()
	slog.Info("Prometheus sink HTTP server started")
}

// Stop gracefully shuts down the self-hosted HTTP server (if running).
func (s *PromScoped) Stop() {
	s.p.stopHTTP()
	slog.Info("Prometheus sink HTTP server stopped")
}

// WithTimestamp is accepted for interface compatibility; Prometheus ignores per-sample timestamps.
func (s *PromScoped) WithTimestamp(int64) sinks.MetricSink {
	return &PromScoped{p: s.p, base: s.base}
}

func (s *PromScoped) WithTags(t sinks.Tags) sinks.MetricSink {
	return &PromScoped{p: s.p, base: mergeTags(s.base, t)}
}

// Handler returns a http.Handler that serves metrics for embedding in your own server.
func (s *PromScoped) Handler() http.Handler {
	return promhttp.HandlerFor(s.p.reg, promhttp.HandlerOpts{})
}

// SetHealthCheck mounts h at path on this sink's HTTP server. Call before
// Start(); it has no effect on a server that's already running.
func (s *PromScoped) SetHealthCheck(path string, h http.Handler) {
	s.p.srvMu.Lock()
	defer s.p.srvMu.Unlock()
	s.p.healthPath = path
	s.p.healthHandler = h
}

// Metric implementations -------------------------------------------------------

func (s *PromScoped) Inc(name, unit string, value float64, tags sinks.Tags) {
	if value <= 0 {
		return // Prom counters must not decrement; ignore non-positive
	}
	mName := s.p.counterName(name, unit)
	t := mergeTags(s.base, tags)
	s.p.write(kindCounter, mName, t, func(v prom.Collector, lbls []string) {
		v.(*prom.CounterVec).With(labelValues(lbls, t)).Add(value)
	})
}

func (s *PromScoped) Rate(name, unit string, value float64, tags sinks.Tags) {
	// Record as a counter; compute rate in PromQL: rate(<name>_total[5m])
	s.Inc(name, unit, value, tags)
}

func (s *PromScoped) Gauge(name, unit string, value float64, tags sinks.Tags) {
	mName := s.p.gaugeName(name, unit)
	t := mergeTags(s.base, tags)
	s.p.write(kindGauge, mName, t, func(v prom.Collector, lbls []string) {
		v.(*prom.GaugeVec).With(labelValues(lbls, t)).Set(value)
	})
}

func (s *PromScoped) Timing(name string, d time.Duration, tags sinks.Tags) {
	// Observe seconds in a histogram
	mName := s.p.histoName(name, "seconds")
	t := mergeTags(s.base, tags)
	s.p.write(kindHistogram, mName, t, func(v prom.Collector, lbls []string) {
		v.(*prom.HistogramVec).With(labelValues(lbls, t)).Observe(d.Seconds())
	})
}

// ---------- Core registry & vectors (shared across scopes) ----------

func (p *promCore) fqName(raw string) string {
	base := sanitizeMetricName(raw)
	ns := sanitizeNS(p.opts.Namespace)
	ss := sanitizeNS(p.opts.Subsystem)
	switch {
	case ns != "" && ss != "":
		return ns + "_" + ss + "_" + base
	case ns != "":
		return ns + "_" + base
	case ss != "":
		return ss + "_" + base
	default:
		return base
	}
}

func (p *promCore) counterName(name, unit string) string {
	s := p.fqName(name)
	if !strings.HasSuffix(s, "_total") {
		s += "_total"
	}
	if unit = sanitizeUnit(unit); unit != "" && !strings.HasSuffix(s, "_"+unit) {
		s += "_" + unit
	}
	return s
}
func (p *promCore) gaugeName(name, unit string) string {
	s := p.fqName(name)
	if unit = sanitizeUnit(unit); unit != "" && !strings.HasSuffix(s, "_"+unit) {
		s += "_" + unit
	}
	return s
}
func (p *promCore) histoName(name, unit string) string {
	// Histograms are not *_total; they export multiple series (_bucket/_sum/_count).
	return p.gaugeName(name, unit)
}

const (
	kindCounter   = "counter"
	kindGauge     = "gauge"
	kindHistogram = "histogram"
)

// write runs fn with the vector and label names for a write of kind to
// name. A write whose tag keys are all known shares the read lock with other
// writes (the vectors are safe for concurrent use). A write that must create
// the metric or widen its labels takes the write lock, so widening never runs
// while another write is in flight.
func (p *promCore) write(kind, name string, t sinks.Tags, fn func(v prom.Collector, labels []string)) {
	if p.writeKnown(kind, name, t, fn) {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	sl := p.slotFor(kind, name, t)
	fn(sl.cur.Load().(prom.Collector), sl.labels)
}

// writeKnown is write's read-lock path; it reports whether it ran fn.
func (p *promCore) writeKnown(kind, name string, t sinks.Tags, fn func(v prom.Collector, labels []string)) bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	sl := p.slots[name]
	if sl == nil || sl.kind != kind || !covers(sl.labels, t) {
		return false
	}
	fn(sl.cur.Load().(prom.Collector), sl.labels)
	return true
}

// covers reports whether every key of t is in the sorted list labels.
func covers(labels []string, t sinks.Tags) bool {
	for k := range t {
		if i := sort.SearchStrings(labels, k); i == len(labels) || labels[i] != k {
			return false
		}
	}
	return true
}

// slot is registered once per metric name and forwards collection to its
// current vector. The registry keeps a name's label names for the life of
// the process, even across Unregister, so widen swaps the vector here.
// kind and labels are guarded by p.mu.
type slot struct {
	kind   string       // one metric type per name, as the registry enforced
	labels []string     // sorted label names of the current vector
	cur    atomic.Value // holds the current vector (always the same Go type)
}

func (s *slot) Describe(chan<- *prom.Desc) {}

func (s *slot) Collect(ch chan<- prom.Metric) {
	if c, ok := s.cur.Load().(prom.Collector); ok {
		c.Collect(ch)
	}
}

// slotFor returns name's slot, creating and registering it on first use and
// widening its labels when t carries a key not seen before. Missing keys are
// written as "". Slots declare no descriptors, so the registry can't reject a
// name reused for another metric type; slotFor panics instead, as
// MustRegister did. (A clash with a collector outside the sink, e.g. go_*,
// shows up at scrape time.) The caller holds p.mu.
func (p *promCore) slotFor(kind, name string, t sinks.Tags) *slot {
	sl := p.slots[name]
	if sl == nil {
		sl = &slot{kind: kind, labels: sortedKeys(t)}
		sl.cur.Store(p.newVec(kind, name, sl.labels))
		p.reg.MustRegister(sl)
		p.slots[name] = sl
		return sl
	}
	if sl.kind != kind {
		panic(fmt.Sprintf("prometheus sink: %s is already registered as a %s, can't register it as a %s", name, sl.kind, kind))
	}
	if !covers(sl.labels, t) {
		p.widen(sl, name, t)
	}
	return sl
}

// widen rebuilds sl's vector with the label names widened to include t's
// keys. Counter and gauge series carry over with "" for the new labels, which
// Prometheus stores as the same series, so nothing resets. Histograms can't
// be restored and start over. A carried-over counter gets a new created
// timestamp, seen only by opt-in created-timestamp ingestion. The new vector
// is filled before it is swapped in. The caller holds p.mu.
func (p *promCore) widen(sl *slot, name string, t sinks.Tags) {
	all := make(sinks.Tags, len(sl.labels)+len(t))
	for _, k := range sl.labels {
		all[k] = ""
	}
	for k := range t {
		all[k] = ""
	}
	labels := sortedKeys(all)
	old := sl.cur.Load().(prom.Collector)
	nv := p.newVec(sl.kind, name, labels)
	switch v := nv.(type) {
	case *prom.CounterVec:
		for _, m := range collectSeries(old) {
			v.With(widenedLabels(m, labels)).Add(m.GetCounter().GetValue())
		}
	case *prom.GaugeVec:
		for _, m := range collectSeries(old) {
			v.With(widenedLabels(m, labels)).Set(m.GetGauge().GetValue())
		}
	}
	sl.labels = labels
	sl.cur.Store(nv)
}

// newVec returns an unregistered vector of kind for name.
func (p *promCore) newVec(kind, name string, labelNames []string) prom.Collector {
	switch kind {
	case kindCounter:
		return prom.NewCounterVec(prom.CounterOpts{Name: name, Help: name}, labelNames)
	case kindGauge:
		return prom.NewGaugeVec(prom.GaugeOpts{Name: name, Help: name}, labelNames)
	default:
		return prom.NewHistogramVec(prom.HistogramOpts{
			Name:    name,
			Help:    name,
			Buckets: p.opts.HistogramBuckets,
		}, labelNames)
	}
}

// collectSeries reads the current series of a vector.
func collectSeries(c prom.Collector) []*dto.Metric {
	ch := make(chan prom.Metric)
	go func() { c.Collect(ch); close(ch) }()
	var out []*dto.Metric
	for m := range ch {
		var d dto.Metric
		if err := m.Write(&d); err == nil {
			out = append(out, &d)
		}
	}
	return out
}

// widenedLabels returns m's label values over labels, "" where m has none.
func widenedLabels(m *dto.Metric, labels []string) prom.Labels {
	out := make(prom.Labels, len(labels))
	for _, k := range labels {
		out[k] = ""
	}
	for _, lp := range m.GetLabel() {
		out[lp.GetName()] = lp.GetValue()
	}
	return out
}

// ---------- HTTP server management ----------

func (p *promCore) startHTTP() {
	p.srvMu.Lock()
	defer p.srvMu.Unlock()
	if p.running {
		return
	}
	// defaults
	addr := p.opts.ListenAddr
	if addr == "" {
		addr = ":2112"
	}
	path := p.opts.MetricsPath
	if path == "" {
		path = "/metrics"
	}
	sdTO := p.opts.ShutdownTimeout
	if sdTO == 0 {
		sdTO = 5 * time.Second
	}

	// mux with /metrics
	mux := http.NewServeMux()
	mux.Handle(path, promhttp.HandlerFor(p.reg, promhttp.HandlerOpts{}))

	if p.healthPath != "" && p.healthHandler != nil {
		mux.Handle(p.healthPath, p.healthHandler)
	}

	// listener
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		// best-effort: do not mark running if we failed to bind
		return
	}

	// server
	srv := &http.Server{
		Handler:      mux,
		ReadTimeout:  p.opts.ReadTimeout,
		WriteTimeout: p.opts.WriteTimeout,
		IdleTimeout:  p.opts.IdleTimeout,
		// Addr is not required when serving on a Listener
	}

	p.ln = ln
	p.srv = srv
	p.running = true
	p.srvWg.Add(1)
	go func() {
		defer p.srvWg.Done()
		_ = srv.Serve(ln) // returns on Shutdown/Close; ignore error
		_ = sdTO          // keep sdTO referenced; actual use is in stopHTTP
	}()
}

func (p *promCore) stopHTTP() {
	p.srvMu.Lock()
	if !p.running {
		p.srvMu.Unlock()
		return
	}
	srv := p.srv
	ln := p.ln
	p.srv = nil
	p.ln = nil
	p.running = false
	sdTO := p.opts.ShutdownTimeout
	if sdTO == 0 {
		sdTO = 5 * time.Second
	}
	p.srvMu.Unlock()

	// graceful shutdown
	ctx, cancel := context.WithTimeout(context.Background(), sdTO)
	defer cancel()
	_ = srv.Shutdown(ctx) // best-effort; ignore error
	_ = ln.Close()        // in case Shutdown didn't close it
	p.srvWg.Wait()
}

// ---------- utils ----------

func mergeTags(a, b sinks.Tags) sinks.Tags {
	if len(a) == 0 {
		out := make(sinks.Tags, len(b))
		for k, v := range b {
			out[sanitizeLabelKey(k)] = v
		}
		return out
	}
	out := make(sinks.Tags, len(a)+len(b))
	for k, v := range a {
		out[sanitizeLabelKey(k)] = v
	}
	for k, v := range b {
		out[sanitizeLabelKey(k)] = v
	}
	return out
}

func sortedKeys(t sinks.Tags) []string {
	keys := make([]string, 0, len(t))
	for k := range t {
		keys = append(keys, sanitizeLabelKey(k))
	}
	sort.Strings(keys)
	return keys
}

func labelValues(keys []string, t sinks.Tags) prom.Labels {
	lbls := make(prom.Labels, len(keys))
	for _, k := range keys {
		lbls[k] = t[k] // missing → "", OK
	}
	return lbls
}

var (
	reMetric = regexp.MustCompile(`[^a-zA-Z0-9_:]`)
	reNS     = regexp.MustCompile(`[^a-zA-Z0-9_]`)
	reLabelK = regexp.MustCompile(`[^a-zA-Z0-9_]`)
)

func sanitizeMetricName(s string) string {
	s = strings.ToLower(s)
	s = strings.ReplaceAll(s, ".", "_")
	s = reMetric.ReplaceAllString(s, "_")
	if s == "" || (s[0] >= '0' && s[0] <= '9') {
		s = "m_" + s
	}
	return s
}
func sanitizeUnit(u string) string {
	u = strings.ToLower(strings.TrimSpace(u))
	u = strings.ReplaceAll(u, "/", "_per_")
	u = strings.ReplaceAll(u, "-", "_")
	return reLabelK.ReplaceAllString(u, "_")
}
func sanitizeNS(s string) string {
	s = strings.ToLower(strings.TrimSpace(s))
	return reNS.ReplaceAllString(s, "_")
}
func sanitizeLabelKey(k string) string {
	k = strings.ToLower(strings.TrimSpace(k))
	k = reLabelK.ReplaceAllString(k, "_")
	if k == "" || (k[0] >= '0' && k[0] <= '9') {
		k = "l_" + k
	}
	return k
}
