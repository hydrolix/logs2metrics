package prometheus

import (
	"fmt"
	"io"
	"net/http/httptest"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mercereau/hydrolix-metrics-go/internal/sinks"
)

// scrapeLines returns the non-comment /metrics lines whose name starts with
// prefix, sorted.
func scrapeLines(t *testing.T, s *PromScoped, prefix string) []string {
	t.Helper()
	rec := httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, httptest.NewRequest("GET", "/metrics", nil))
	b, err := io.ReadAll(rec.Body)
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for _, l := range strings.Split(string(b), "\n") {
		if strings.HasPrefix(l, prefix) {
			out = append(out, l)
		}
	}
	sort.Strings(out)
	return out
}

func assertLines(t *testing.T, got, want []string) {
	t.Helper()
	sort.Strings(want)
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Fatalf("/metrics lines:\n  %s\nwant:\n  %s", strings.Join(got, "\n  "), strings.Join(want, "\n  "))
	}
}

// A tag key that first appears after a counter's first write must become a
// label, and the series written before keep their values (with the new
// label empty, which Prometheus stores as the same series).
func TestCounterGainsLabelAfterFirstWrite(t *testing.T) {
	s := NewSink(PromOpts{})
	for range 3 {
		s.Inc("collector.poll", "total", 1, sinks.Tags{"query": "q1", "status": "success"})
	}
	s.Inc("collector.poll", "total", 1, sinks.Tags{"query": "q1", "status": "error", "reason": "auth"})
	s.Inc("collector.poll", "total", 1, sinks.Tags{"query": "q1", "status": "error", "reason": "query"})

	assertLines(t, scrapeLines(t, s, "collector_poll_total"), []string{
		`collector_poll_total{query="q1",reason="",status="success"} 3`,
		`collector_poll_total{query="q1",reason="auth",status="error"} 1`,
		`collector_poll_total{query="q1",reason="query",status="error"} 1`,
	})
}

// A first row with a null dimension (so no app tag) must not lock the app
// label out: every later app gets its own series instead of overwriting one.
func TestGaugeGainsLabelAfterNullDimensionRow(t *testing.T) {
	s := NewSink(PromOpts{})
	s.Gauge("cluster.log_lines", "line", 5, sinks.Tags{"level": "INFO"})
	s.Gauge("cluster.log_lines", "line", 10, sinks.Tags{"level": "INFO", "app": "query-head"})
	s.Gauge("cluster.log_lines", "line", 20, sinks.Tags{"level": "INFO", "app": "intake"})

	assertLines(t, scrapeLines(t, s, "cluster_log_lines_line"), []string{
		`cluster_log_lines_line{app="",level="INFO"} 5`,
		`cluster_log_lines_line{app="query-head",level="INFO"} 10`,
		`cluster_log_lines_line{app="intake",level="INFO"} 20`,
	})
}

// Several new keys in one write widen the label set once, with all of them.
func TestSeveralNewKeysAtOnce(t *testing.T) {
	s := NewSink(PromOpts{})
	s.Inc("m", "total", 2, sinks.Tags{"a": "1"})
	s.Inc("m", "total", 1, sinks.Tags{"a": "1", "b": "2", "c": "3"})

	assertLines(t, scrapeLines(t, s, "m_total"), []string{
		`m_total{a="1",b="",c=""} 2`,
		`m_total{a="1",b="2",c="3"} 1`,
	})
}

// A key present at first and missing later is still written as "".
func TestMissingKeyStillWrittenEmpty(t *testing.T) {
	s := NewSink(PromOpts{})
	s.Gauge("g", "", 1, sinks.Tags{"a": "1", "b": "2"})
	s.Gauge("g", "", 7, sinks.Tags{"a": "1"})

	assertLines(t, scrapeLines(t, s, "g{"), []string{
		`g{a="1",b=""} 7`,
		`g{a="1",b="2"} 1`,
	})
}

// A histogram that gains a label keeps working; its earlier observations
// are not carried over (the Prometheus client can't restore them).
func TestHistogramGainsLabel(t *testing.T) {
	s := NewSink(PromOpts{})
	s.Timing("poll.duration", time.Second, sinks.Tags{"query": "q1"})
	s.Timing("poll.duration", 2*time.Second, sinks.Tags{"query": "q1", "reason": "auth"})

	got := scrapeLines(t, s, "poll_duration_seconds_count")
	assertLines(t, got, []string{
		`poll_duration_seconds_count{query="q1",reason="auth"} 1`,
	})
}

// Writes racing a label widening must never be lost. Run with -race.
func TestConcurrentWritesDuringWidening(t *testing.T) {
	s := NewSink(PromOpts{})
	const writers, perWriter = 8, 200
	var wg sync.WaitGroup
	for w := range writers {
		wg.Go(func() {
			for i := range perWriter {
				tags := sinks.Tags{"w": strconv.Itoa(w)}
				if i%50 == 0 { // new keys keep appearing mid-stream
					tags["k"+strconv.Itoa(i/50)] = "x"
				}
				s.Inc("c", "total", 1, tags)
			}
		})
	}
	wg.Wait()

	total := 0.0
	for _, l := range scrapeLines(t, s, "c_total") {
		v, err := strconv.ParseFloat(l[strings.LastIndex(l, " ")+1:], 64)
		if err != nil {
			t.Fatalf("bad line %q: %v", l, err)
		}
		total += v
	}
	if total != writers*perWriter {
		t.Fatalf("counter total = %v, want %d: writes were lost during widening", total, writers*perWriter)
	}
}

// Reusing a name for a second metric type is a programming error. Before
// label widening the registry rejected it at the second write (MustRegister
// panics); that check must survive, rather than failing every scrape later.
func TestNameClashAcrossTypesStillPanics(t *testing.T) {
	s := NewSink(PromOpts{})
	s.Gauge("x", "seconds", 1, nil) // gauge x_seconds
	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("a histogram reusing the gauge's name x_seconds should panic, as before")
		}
		// The sink's own check, naming the clash, not an incidental panic.
		if msg := fmt.Sprint(r); !strings.Contains(msg, "x_seconds is already registered as a gauge") {
			t.Fatalf("panic = %q, want the sink's name-clash message", msg)
		}
	}()
	s.Timing("x", time.Second, nil) // histogram x_seconds
}
