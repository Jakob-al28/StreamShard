package aggregate

import (
	"sort"
	"time"

	"github.com/jakob-al28/StreamShard/internal/log"
)

type bucket struct {
	key   string
	count uint64
}

type Aggregate struct {
	window   time.Duration
	topK     int
	counts   map[string]uint64
	timeline []timedKey
}

type timedKey struct {
	key string
	at  time.Time
}

type TopEntry struct {
	Key   string
	Count uint64
}

func New(window time.Duration, topK int) *Aggregate {
	return &Aggregate{
		window: window,
		topK:   topK,
		counts: make(map[string]uint64),
	}
}

func (a *Aggregate) Apply(e log.Entry) {
	a.evict(e.Timestamp)
	a.counts[e.Key]++
	a.timeline = append(a.timeline, timedKey{key: e.Key, at: e.Timestamp})
}

func (a *Aggregate) Counts() map[string]uint64 {
	a.evict(time.Now())
	out := make(map[string]uint64, len(a.counts))
	for k, v := range a.counts {
		out[k] = v
	}
	return out
}

func (a *Aggregate) TopK() []TopEntry {
	a.evict(time.Now())
	buckets := make([]bucket, 0, len(a.counts))
	for k, c := range a.counts {
		buckets = append(buckets, bucket{k, c})
	}
	sort.Slice(buckets, func(i, j int) bool {
		return buckets[i].count > buckets[j].count
	})
	k := a.topK
	if k > len(buckets) {
		k = len(buckets)
	}
	out := make([]TopEntry, k)
	for i := range out {
		out[i] = TopEntry{Key: buckets[i].key, Count: buckets[i].count}
	}
	return out
}

func (a *Aggregate) Rebuild(entries []log.Entry) {
	a.counts = make(map[string]uint64)
	a.timeline = a.timeline[:0]
	for _, e := range entries {
		a.Apply(e)
	}
}

func (a *Aggregate) Snapshot(baseOffset uint64) log.Snapshot {
	a.evict(time.Now())
	counts := make(map[string]uint64, len(a.counts))
	for k, v := range a.counts {
		counts[k] = v
	}
	timeline := make([]log.SnapshotEntry, len(a.timeline))
	for i, t := range a.timeline {
		timeline[i] = log.SnapshotEntry{Key: t.key, At: t.at}
	}
	return log.Snapshot{
		BaseOffset: baseOffset,
		Counts:     counts,
		Timeline:   timeline,
	}
}

func (a *Aggregate) LoadSnapshot(s log.Snapshot) {
	a.counts = s.Counts
	if a.counts == nil {
		a.counts = make(map[string]uint64)
	}
	a.timeline = make([]timedKey, len(s.Timeline))
	for i, t := range s.Timeline {
		a.timeline[i] = timedKey{key: t.Key, at: t.At}
	}
}

func (a *Aggregate) evict(now time.Time) {
	cutoff := now.Add(-a.window)
	i := 0
	for i < len(a.timeline) && a.timeline[i].at.Before(cutoff) {
		a.counts[a.timeline[i].key]--
		if a.counts[a.timeline[i].key] == 0 {
			delete(a.counts, a.timeline[i].key)
		}
		i++
	}
	a.timeline = a.timeline[i:]
}
