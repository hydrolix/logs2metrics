package otel

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mercereau/hydrolix-metrics-go/internal/sinks"
)

// syncBuffer is a bytes.Buffer safe for concurrent writes from slog.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// captureLogs routes the default slog logger into a buffer for the test.
func captureLogs(t *testing.T) *syncBuffer {
	t.Helper()
	buf := &syncBuffer{}
	orig := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(buf, nil)))
	t.Cleanup(func() { slog.SetDefault(orig) })
	return buf
}

// recSink is a self-sink that records counter increments by name and tags.
type recSink struct {
	mu   *sync.Mutex
	incs *[]recInc
	base sinks.Tags
}

type recInc struct {
	name string
	tags sinks.Tags
}

func newRecSink() recSink {
	return recSink{mu: &sync.Mutex{}, incs: &[]recInc{}}
}

func (r recSink) Name() string                              { return "rec" }
func (r recSink) Start()                                    {}
func (r recSink) Stop()                                     {}
func (r recSink) Gauge(string, string, float64, sinks.Tags) {}
func (r recSink) Rate(string, string, float64, sinks.Tags)  {}
func (r recSink) Timing(string, time.Duration, sinks.Tags)  {}
func (r recSink) WithTimestamp(int64) sinks.MetricSink      { return r }
func (r recSink) WithTags(t sinks.Tags) sinks.MetricSink {
	return recSink{mu: r.mu, incs: r.incs, base: sinks.MergeTags(r.base, t)}
}
func (r recSink) Inc(name, _ string, _ float64, tags sinks.Tags) {
	r.mu.Lock()
	*r.incs = append(*r.incs, recInc{name: name, tags: sinks.MergeTags(r.base, tags)})
	r.mu.Unlock()
}

// count returns how many increments of name carry every tag in want.
func (r recSink) count(name string, want sinks.Tags) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	n := 0
	for _, in := range *r.incs {
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

// Every OTLP export - periodic ones, empty ones and the final one at
// shutdown - must be counted as an export on the self-sink, so an exporter
// that stops delivering to its collector is visible from outside. Successes
// fire every interval, so they double as a heartbeat.
func TestEveryExportIsCounted(t *testing.T) {
	mem := useMemExporter(t)
	self := newRecSink()
	s := NewOTelSink(OTelOpts{ExportInterval: 20 * time.Millisecond, SelfSink: self})

	s.Inc("hydrolix.collector.poll", "total", 1, nil)
	s.WithTimestamp(1_788_372_780).Gauge("cluster.log_lines", "line", 95, nil)

	deadline := time.Now().Add(2 * time.Second)
	for mem.exportCount() < 3 {
		if time.Now().After(deadline) {
			t.Fatalf("expected at least 3 exports within 2s at a 20ms interval, got %d", mem.exportCount())
		}
		time.Sleep(10 * time.Millisecond)
	}
	s.Stop()

	exports := mem.exportCount()
	ok := self.count("hydrolix.sink.export", sinks.Tags{"sink": "otel", "status": "success"})
	if ok != exports {
		t.Fatalf("every export must be counted as success: exporter saw %d exports, success counter = %d", exports, ok)
	}
	if bad := self.count("hydrolix.sink.export", sinks.Tags{"status": "error"}); bad != 0 {
		t.Fatalf("no export failed, but error counter = %d", bad)
	}
}

// A failed export is counted as an error, and the error still reaches the
// caller unchanged: here the final export at shutdown, whose error Stop
// returns.
func TestFailedExportIsCountedAndReturned(t *testing.T) {
	mem := useMemExporter(t)
	unreachable := errors.New("collector unreachable")
	mem.failWith(unreachable)
	self := newRecSink()
	s := NewOTelSink(OTelOpts{ExportInterval: time.Hour, SelfSink: self})

	s.WithTimestamp(1_788_372_780).Gauge("cluster.log_lines", "line", 95, nil)
	err := s.c.stop(context.Background()) // final export, which fails

	if !errors.Is(err, unreachable) {
		t.Fatalf("the export error must reach the caller unchanged, got %v", err)
	}
	if bad := self.count("hydrolix.sink.export", sinks.Tags{"sink": "otel", "status": "error"}); bad != 1 {
		t.Fatalf("the failed export must be counted once as status=error, got %d", bad)
	}
	if ok := self.count("hydrolix.sink.export", sinks.Tags{"status": "success"}); ok != 0 {
		t.Fatalf("every export failed, but success counter = %d", ok)
	}
}

// A failed export must also show up as an ERROR log line: log-based alerts
// see nothing otherwise, because the SDK's default error handler is silent.
// A successful export logs nothing at ERROR.
func TestFailedExportIsLoggedAtError(t *testing.T) {
	logs := captureLogs(t)
	mem := useMemExporter(t)
	s := NewOTelSink(OTelOpts{ExportInterval: time.Hour})

	s.WithTimestamp(1_788_372_780).Gauge("cluster.log_lines", "line", 95, nil)
	_ = s.c.reader.ForceFlush(context.Background()) // a successful export
	if got := logs.String(); strings.Contains(got, "level=ERROR") {
		t.Fatalf("a successful export must not log at ERROR, got:\n%s", got)
	}

	mem.failWith(errors.New("collector unreachable"))
	s.WithTimestamp(1_788_372_840).Gauge("cluster.log_lines", "line", 96, nil)
	_ = s.c.stop(context.Background()) // final export, which fails

	got := logs.String()
	if !strings.Contains(got, "level=ERROR") || !strings.Contains(got, "collector unreachable") {
		t.Fatalf("a failed export must log at ERROR with the error, got:\n%s", got)
	}
}
