package log

import (
	"testing"
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
