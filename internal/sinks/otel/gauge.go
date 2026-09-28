package otel

import (
	"context"
	"sort"
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/instrumentation"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// gaugeBuffer accumulates gauge observations for manual OTLP export.
//
// The collector re-polls a sliding window, so the same (series, event time)
// arrives repeatedly - usually with the same value, occasionally corrected by
// late data. Keeping one point per (series, timestamp) with last-write-wins
// makes that re-emission idempotent, mirroring what timestamped backends do.
//
// Gauges bypass the SDK Meter entirely because synchronous SDK instruments
// always stamp observations with "now": there is no way to record a datapoint
// at its event time through the Meter API. Instead the buffer is an external
// sdkmetric.Producer: the PeriodicReader drains it on every collect and exports
// its hand-built metricdata alongside the SDK instruments, so the query's
// values and timestamps stay intact and shutdown's final export (bounded by
// the reader's timeout) includes them.
type gaugeBuffer struct {
	mu     sync.Mutex
	points map[gaugeKey]gaugePoint
}

type gaugeKey struct {
	name  string
	unit  string
	attrs attribute.Distinct // comparable identity of the attribute set
	ts    int64              // unix nanos
}

type gaugePoint struct {
	name  string
	unit  string
	attrs attribute.Set
	t     time.Time
	value float64
}

func newGaugeBuffer() *gaugeBuffer {
	return &gaugeBuffer{points: map[gaugeKey]gaugePoint{}}
}

func (b *gaugeBuffer) add(name, unit string, kvs []attribute.KeyValue, t time.Time, value float64) {
	set := attribute.NewSet(kvs...)
	k := gaugeKey{name: name, unit: unit, attrs: set.Equivalent(), ts: t.UnixNano()}
	b.mu.Lock()
	b.points[k] = gaugePoint{name: name, unit: unit, attrs: set, t: t, value: value}
	b.mu.Unlock()
}

// Produce implements sdkmetric.Producer. A failed export drops what it
// drained; beyond the OTLP exporter's own retries, those points come back
// only if a later poll re-reads their bucket.
func (b *gaugeBuffer) Produce(context.Context) ([]metricdata.ScopeMetrics, error) {
	metrics := b.drain()
	if len(metrics) == 0 {
		return nil, nil
	}
	return []metricdata.ScopeMetrics{{
		Scope:   instrumentation.Scope{Name: "cdn-metrics"},
		Metrics: metrics,
	}}, nil
}

// drain empties the buffer and returns its contents grouped per metric,
// ordered by name then unit, each metric's points oldest first. Order matters
// to backends that drop a point older than the series' latest (Splunk
// Observability does): a backfill carrying several minutes of one series
// must arrive in time order or all but the newest minute are lost.
func (b *gaugeBuffer) drain() []metricdata.Metrics {
	b.mu.Lock()
	points := b.points
	b.points = map[gaugeKey]gaugePoint{}
	b.mu.Unlock()

	if len(points) == 0 {
		return nil
	}

	type metricKey struct{ name, unit string }
	grouped := map[metricKey][]metricdata.DataPoint[float64]{}
	for _, p := range points {
		mk := metricKey{p.name, p.unit}
		grouped[mk] = append(grouped[mk], metricdata.DataPoint[float64]{
			Attributes: p.attrs,
			Time:       p.t,
			Value:      p.value,
		})
	}

	keys := make([]metricKey, 0, len(grouped))
	for mk := range grouped {
		keys = append(keys, mk)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].name != keys[j].name {
			return keys[i].name < keys[j].name
		}
		return keys[i].unit < keys[j].unit
	})

	out := make([]metricdata.Metrics, 0, len(keys))
	for _, mk := range keys {
		dps := grouped[mk]
		sort.SliceStable(dps, func(i, j int) bool { return dps[i].Time.Before(dps[j].Time) })
		out = append(out, metricdata.Metrics{
			Name:        mk.name,
			Unit:        mk.unit,
			Description: mk.name,
			Data:        metricdata.Gauge[float64]{DataPoints: dps},
		})
	}
	return out
}
