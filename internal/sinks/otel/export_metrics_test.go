package otel

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/mercereau/hydrolix-metrics-go/internal/sinks"
)

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

// Every OTLP export - the periodic reader's (counters, histograms) and the
// gauge flusher's - must be counted as an export on the self-sink, so an
// exporter that stops delivering to its collector is visible from outside.
func TestEveryExportIsCounted(t *testing.T) {
	mem := useMemExporter(t)
	self := newRecSink()
	s := NewOTelSink(OTelOpts{ExportInterval: 20 * time.Millisecond, SelfSink: self})

	s.Inc("hydrolix.collector.poll", "total", 1, nil)                          // periodic reader path
	s.WithTimestamp(1_788_372_780).Gauge("cluster.log_lines", "line", 95, nil) // gauge flusher path

	deadline := time.Now().Add(2 * time.Second)
	for {
		calls, gaugeCalls := mem.callCounts()
		if gaugeCalls >= 1 && calls > gaugeCalls {
			break // both paths have exported at least once
		}
		if time.Now().After(deadline) {
			t.Fatalf("expected exports from both paths within 2s, got calls=%d gaugeCalls=%d", calls, gaugeCalls)
		}
		time.Sleep(10 * time.Millisecond)
	}
	s.Stop()

	calls, _ := mem.callCounts()
	ok := self.count("hydrolix.sink.export", sinks.Tags{"sink": "otel", "status": "success"})
	if ok != calls {
		t.Fatalf("every export must be counted as success: exporter saw %d calls, success counter = %d", calls, ok)
	}
	if bad := self.count("hydrolix.sink.export", sinks.Tags{"status": "error"}); bad != 0 {
		t.Fatalf("no export failed, but error counter = %d", bad)
	}
}

// A failed export is counted as an error, and the error still reaches the
// caller: the gauge flusher must keep reporting its own send failure.
func TestFailedExportIsCountedAndReturned(t *testing.T) {
	mem := useMemExporter(t)
	mem.failWith(errors.New("collector unreachable"))
	self := newRecSink()
	s := NewOTelSink(OTelOpts{ExportInterval: time.Hour, SelfSink: self})

	s.WithTimestamp(1_788_372_780).Gauge("cluster.log_lines", "line", 95, nil)
	s.Stop() // final gauge flush, which fails

	if bad := self.count("hydrolix.sink.export", sinks.Tags{"sink": "otel", "status": "error"}); bad < 1 {
		t.Fatalf("a failed export must be counted as status=error, got %d", bad)
	}
	if ok := self.count("hydrolix.sink.export", sinks.Tags{"status": "success"}); ok != 0 {
		t.Fatalf("every export failed, but success counter = %d", ok)
	}
	if sf := self.count("hydrolix.sink.send_failures", nil); sf < 1 {
		t.Fatal("the export error must still reach the gauge flusher (send_failures not incremented)")
	}
}
