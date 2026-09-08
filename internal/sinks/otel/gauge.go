package otel

import (
	"sort"
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"
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
// at its event time through the Meter API. Exporting hand-built metricdata
// keeps the query's values and timestamps intact.
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

// drain empties the buffer and returns its contents grouped per metric,
// deterministically ordered by name then unit.
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
		out = append(out, metricdata.Metrics{
			Name:        mk.name,
			Unit:        mk.unit,
			Description: mk.name,
			Data:        metricdata.Gauge[float64]{DataPoints: grouped[mk]},
		})
	}
	return out
}
