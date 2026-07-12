package log

import (
	"strconv"
	"testing"
	"time"
)

func TestAppendAndOffset(t *testing.T) {
	l := New()

	e, ok := l.Append("id-1", "key-a", []byte("v1"))
	if !ok || e.Offset != 0 {
		t.Fatalf("expected offset 0, got %d ok=%v", e.Offset, ok)
	}

	e, ok = l.Append("id-2", "key-b", []byte("v2"))
	if !ok || e.Offset != 1 {
		t.Fatalf("expected offset 1, got %d ok=%v", e.Offset, ok)
	}

	if l.Head() != 2 {
		t.Fatalf("expected head 2, got %d", l.Head())
	}
}

func TestIdempotentDedup(t *testing.T) {
	l := New()
	l.Append("id-1", "key-a", []byte("v1"))

	_, ok := l.Append("id-1", "key-a", []byte("v1"))
	if ok {
		t.Fatal("duplicate append should return ok=false")
	}

	if l.Head() != 1 {
		t.Fatalf("log should still have 1 entry, got %d", l.Head())
	}
}

func TestSince(t *testing.T) {
	l := New()
	l.Append("id-1", "k", []byte("a"))
	l.Append("id-2", "k", []byte("b"))
	l.Append("id-3", "k", []byte("c"))

	slice := l.Since(1)
	if len(slice) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(slice))
	}
	if slice[0].Offset != 1 || slice[1].Offset != 2 {
		t.Fatalf("unexpected offsets: %v", slice)
	}
}

func TestSincePastHead(t *testing.T) {
	l := New()
	l.Append("id-1", "k", []byte("a"))

	if l.Since(99) != nil {
		t.Fatal("expected nil for offset past head")
	}
}

func TestCompactLeavesDedupIntact(t *testing.T) {
	l, err := Open(t.TempDir())
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	for i := 0; i < 5; i++ {
		l.Append(string(rune('a'+i)), "k", []byte("v"))
	}

	if err := l.Compact(Snapshot{BaseOffset: 3}); err != nil {
		t.Fatalf("compact: %v", err)
	}

	if _, ok := l.Append("a", "k", []byte("v")); ok {
		t.Fatal("replay of compacted-but-still-fresh id should be deduped")
	}
}

func TestDedupSeenWithinTTL(t *testing.T) {
	d := newDedupSet(5 * time.Minute)
	now := time.Unix(1000, 0)

	d.add("x", now)
	if !d.seen("x", now.Add(time.Minute)) {
		t.Fatal("id within TTL should be seen")
	}
}

func TestDedupExpiresAfterTTL(t *testing.T) {
	d := newDedupSet(5 * time.Minute)
	now := time.Unix(1000, 0)

	d.add("x", now)
	if d.seen("x", now.Add(6*time.Minute)) {
		t.Fatal("id past TTL should have expired")
	}
}

func TestDedupBoundedSize(t *testing.T) {
	ttl := 10 * time.Second
	d := newDedupSet(ttl)
	start := time.Unix(0, 0)

	for s := 0; s < 100; s++ {
		now := start.Add(time.Duration(s) * time.Second)
		d.add("id-"+strconv.Itoa(s), now)
		d.evict(now)
	}

	if d.len() > 12 {
		t.Fatalf("dedup set grew past one TTL window: %d ids retained", d.len())
	}
}
