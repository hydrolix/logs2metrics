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

	counters   map[string]*prom.CounterVec
	gauges     map[string]*prom.GaugeVec
	histograms map[string]*prom.HistogramVec
	labels     map[string][]string // label names per metric name, shared by all vector types
	slots      map[string]*slot    // registered collectors, by metric name (see slot)

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
		reg:        prom.NewRegistry(),
		opts:       opts,
		counters:   map[string]*prom.CounterVec{},
		gauges:     map[string]*prom.GaugeVec{},
		histograms: map[string]*prom.HistogramVec{},
		labels:     map[string][]string{},
		slots:      map[string]*slot{},
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
	s.p.write(kindCounter, mName, t, func(lbls []string) {
		s.p.counterVec(mName, lbls).With(labelValues(lbls, t)).Add(value)
	})
}

func (s *PromScoped) Rate(name, unit string, value float64, tags sinks.Tags) {
	// Record as a counter; compute rate in PromQL: rate(<name>_total[5m])
	s.Inc(name, unit, value, tags)
}

func (s *PromScoped) Gauge(name, unit string, value float64, tags sinks.Tags) {
	mName := s.p.gaugeName(name, unit)
	t := mergeTags(s.base, tags)
	s.p.write(kindGauge, mName, t, func(lbls []string) {
		s.p.gaugeVec(mName, lbls).With(labelValues(lbls, t)).Set(value)
	})
}

func (s *PromScoped) Timing(name string, d time.Duration, tags sinks.Tags) {
	// Observe seconds in a histogram
	mName := s.p.histoName(name, "seconds")
	t := mergeTags(s.base, tags)
	s.p.write(kindHistogram, mName, t, func(lbls []string) {
		s.p.histogramVec(mName, lbls).With(labelValues(lbls, t)).Observe(d.Seconds())
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

// write runs fn with the label names to use for a write of kind to name.
// A write whose tag keys are all known and whose vector exists shares the
// read lock with other writes (the vectors are safe for concurrent use). A
// write that must create the vector or widen its labels takes the write
// lock, so widening never runs while another write is in flight.
func (p *promCore) write(kind, name string, t sinks.Tags, fn func(labels []string)) {
	if p.writeKnown(kind, name, t, fn) {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	fn(p.labelsFor(name, t))
}

// writeKnown is write's read-lock path; it reports whether it ran fn.
func (p *promCore) writeKnown(kind, name string, t sinks.Tags, fn func(labels []string)) bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	sl := p.slots[name]
	lbls := p.labels[name]
	if sl == nil || sl.kind != kind || !covers(lbls, t) {
		return false
	}
	fn(lbls)
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

// labelsFor returns the label names for a metric name. The first write sets
// them; a later write carrying keys not seen before widens them, and every
// existing vector of that name is rebuilt (see widen). Missing keys are
// written as "". The caller holds p.mu.
func (p *promCore) labelsFor(metricName string, tags sinks.Tags) []string {
	keys := sortedKeys(tags)
	cur, ok := p.labels[metricName]
	if !ok {
		p.labels[metricName] = keys
		return keys
	}
	merged := unionSorted(cur, keys)
	if len(merged) == len(cur) {
		return cur
	}
	p.widen(metricName, merged)
	p.labels[metricName] = merged
	return merged
}

// widen rebuilds every vector named name with the label names in labels.
// Counter and gauge series are carried over with "" for the new labels:
// Prometheus treats an empty label as absent, so scrapers see the same series
// with the same values, and a counter does not reset. Histogram observations
// can't be restored through the client, so a histogram that gains a label
// starts over. A carried-over counter does get a new created timestamp; only
// Prometheus's opt-in created-timestamp-zero-ingestion (protobuf scrapes)
// reads it, and would then see one reset at the widening. The new vector is
// filled before it is swapped in, so a scrape never sees it half built. The
// caller holds p.mu.
func (p *promCore) widen(name string, labels []string) {
	if cv := p.counters[name]; cv != nil {
		nv := newCounterVec(name, labels)
		for _, m := range collectSeries(cv) {
			nv.With(widenedLabels(m, labels)).Add(m.GetCounter().GetValue())
		}
		p.counters[name] = nv
		p.publish(kindCounter, name, nv)
	}
	if gv := p.gauges[name]; gv != nil {
		nv := newGaugeVec(name, labels)
		for _, m := range collectSeries(gv) {
			nv.With(widenedLabels(m, labels)).Set(m.GetGauge().GetValue())
		}
		p.gauges[name] = nv
		p.publish(kindGauge, name, nv)
	}
	if hv := p.histograms[name]; hv != nil {
		nv := p.newHistogramVec(name, labels)
		p.histograms[name] = nv
		p.publish(kindHistogram, name, nv)
	}
}

// slot is what the registry sees for one metric: it forwards collection to
// the vector currently stored in it. The registry fixes a metric name's
// label names for the life of the process, even across Unregister, so a
// vector can't be re-registered with more labels; a slot declares no
// descriptors (an "unchecked" collector), which lets widen swap the vector
// behind it instead.
type slot struct {
	kind string       // one metric type per name, as the registry enforced
	cur  atomic.Value // holds the current vector (always the same Go type)
}

func (s *slot) Describe(chan<- *prom.Desc) {}

func (s *slot) Collect(ch chan<- prom.Metric) {
	if c, ok := s.cur.Load().(prom.Collector); ok {
		c.Collect(ch)
	}
}

// publish makes c the vector scraped for name, registering its slot on
// first use. Reusing a name for another metric type panics, as MustRegister
// did before slots: slots declare no descriptors, so the registry no longer
// catches it. For the same reason a name shared with a collector registered
// outside the sink (e.g. go_* runtime metrics) fails the scrape instead of
// panicking at registration; the namespace prefix keeps sink names apart.
// The caller holds p.mu.
func (p *promCore) publish(kind, name string, c prom.Collector) {
	sl := p.slots[name]
	if sl == nil {
		sl = &slot{kind: kind}
		p.reg.MustRegister(sl)
		p.slots[name] = sl
	}
	if sl.kind != kind {
		panic(fmt.Sprintf("prometheus sink: %s is already registered as a %s, can't register it as a %s", name, sl.kind, kind))
	}
	sl.cur.Store(c)
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

// unionSorted merges two sorted, duplicate-free lists.
func unionSorted(a, b []string) []string {
	out := make([]string, 0, len(a)+len(b))
	i, j := 0, 0
	for i < len(a) || j < len(b) {
		switch {
		case j == len(b) || (i < len(a) && a[i] < b[j]):
			out = append(out, a[i])
			i++
		case i == len(a) || b[j] < a[i]:
			out = append(out, b[j])
			j++
		default:
			out = append(out, a[i])
			i, j = i+1, j+1
		}
	}
	return out
}

// counterVec, gaugeVec and histogramVec return the vector for name, creating
// and publishing it with labelNames on first use. The caller holds p.mu.
func (p *promCore) counterVec(name string, labelNames []string) *prom.CounterVec {
	if cv := p.counters[name]; cv != nil {
		return cv
	}
	cv := newCounterVec(name, labelNames)
	p.publish(kindCounter, name, cv)
	p.counters[name] = cv
	return cv
}

func (p *promCore) gaugeVec(name string, labelNames []string) *prom.GaugeVec {
	if gv := p.gauges[name]; gv != nil {
		return gv
	}
	gv := newGaugeVec(name, labelNames)
	p.publish(kindGauge, name, gv)
	p.gauges[name] = gv
	return gv
}

func (p *promCore) histogramVec(name string, labelNames []string) *prom.HistogramVec {
	if hv := p.histograms[name]; hv != nil {
		return hv
	}
	hv := p.newHistogramVec(name, labelNames)
	p.publish(kindHistogram, name, hv)
	p.histograms[name] = hv
	return hv
}

func newCounterVec(name string, labelNames []string) *prom.CounterVec {
	return prom.NewCounterVec(prom.CounterOpts{Name: name, Help: name}, labelNames)
}

func newGaugeVec(name string, labelNames []string) *prom.GaugeVec {
	return prom.NewGaugeVec(prom.GaugeOpts{Name: name, Help: name}, labelNames)
}

func (p *promCore) newHistogramVec(name string, labelNames []string) *prom.HistogramVec {
	return prom.NewHistogramVec(prom.HistogramOpts{
		Name:    name,
		Help:    name,
		Buckets: p.opts.HistogramBuckets,
	}, labelNames)
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
