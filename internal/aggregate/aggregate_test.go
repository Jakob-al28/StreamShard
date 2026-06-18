package aggregate

import (
	"testing"
	"time"

	"github.com/jakob-al28/StreamShard/internal/log"
)

func entry(key string, at time.Time) log.Entry {
	return log.Entry{Key: key, Timestamp: at}
}

func TestCounts(t *testing.T) {
	a := New(time.Minute, 5)
	now := time.Now()

	a.Apply(entry("foo", now))
	a.Apply(entry("foo", now))
	a.Apply(entry("bar", now))

	counts := a.Counts()
	if counts["foo"] != 2 {
		t.Fatalf("expected foo=2, got %d", counts["foo"])
	}
	if counts["bar"] != 1 {
		t.Fatalf("expected bar=1, got %d", counts["bar"])
	}
}

func TestWindowEviction(t *testing.T) {
	a := New(time.Second, 5)
	old := time.Now().Add(-2 * time.Second)
	recent := time.Now()

	a.Apply(entry("foo", old))
	a.Apply(entry("foo", recent))

	counts := a.Counts()
	if counts["foo"] != 1 {
		t.Fatalf("expected foo=1 after eviction, got %d", counts["foo"])
	}
}

func TestTopK(t *testing.T) {
	a := New(time.Minute, 2)
	now := time.Now()

	a.Apply(entry("a", now))
	a.Apply(entry("b", now))
	a.Apply(entry("b", now))
	a.Apply(entry("c", now))
	a.Apply(entry("c", now))
	a.Apply(entry("c", now))

	top := a.TopK()
	if len(top) != 2 {
		t.Fatalf("expected 2 top entries, got %d", len(top))
	}
	if top[0].Key != "c" || top[0].Count != 3 {
		t.Fatalf("expected c=3 at top, got %+v", top[0])
	}
	if top[1].Key != "b" || top[1].Count != 2 {
		t.Fatalf("expected b=2 second, got %+v", top[1])
	}
}

func TestRebuild(t *testing.T) {
	a := New(time.Minute, 5)
	now := time.Now()

	entries := []log.Entry{
		entry("x", now),
		entry("x", now),
		entry("y", now),
	}
	a.Rebuild(entries)

	counts := a.Counts()
	if counts["x"] != 2 || counts["y"] != 1 {
		t.Fatalf("unexpected counts after rebuild: %v", counts)
	}
}
