package partition

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestQueueDepthUnderLoad(t *testing.T) {
	p := New(time.Minute, 5, 64)

	var wg sync.WaitGroup
	for i := range 60 {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			p.Apply(fmt.Sprintf("id-%d", i), "key", []byte("v"))
		}(i)
	}
	wg.Wait()

	if p.QueueDepth() != 0 {
		t.Fatalf("queue should drain to 0 after all writes complete, got %d", p.QueueDepth())
	}
}

func TestQueueCap(t *testing.T) {
	p := New(time.Minute, 5, 32)
	if p.QueueCap() != 32 {
		t.Fatalf("expected cap 32, got %d", p.QueueCap())
	}
}

func TestBatchedAndUnbatchedAgree(t *testing.T) {
	plain := Open("", time.Minute, 5, 1024, false, 1)
	batched := Open("", time.Minute, 5, 1024, false, 64)

	apply := func(p *Partition) (created, dups int) {
		for i := range 1000 {
			id := fmt.Sprintf("id-%d", i%250)
			if p.Apply(id, "k", []byte("v")).Fresh {
				created++
			} else {
				dups++
			}
		}
		return
	}

	pc, pd := apply(plain)
	bc, bd := apply(batched)

	if pc != bc || pd != bd {
		t.Fatalf("fresh/dup mismatch: plain=(%d,%d) batched=(%d,%d)", pc, pd, bc, bd)
	}
	if pc != 250 {
		t.Fatalf("expected 250 unique creates, got %d", pc)
	}

	pcounts := plain.Query().Counts
	bcounts := batched.Query().Counts
	if pcounts["k"] != bcounts["k"] {
		t.Fatalf("aggregate mismatch: plain=%d batched=%d", pcounts["k"], bcounts["k"])
	}
}

func TestBatchedDurableWAL(t *testing.T) {
	dir := t.TempDir()
	p := Open(dir, time.Minute, 5, 1024, false, 32)
	for i := range 500 {
		p.Apply(fmt.Sprintf("id-%d", i), "k", []byte("v"))
	}
	if got := len(p.LogSince(0)); got != 500 {
		t.Fatalf("in-memory log: want 500 entries, got %d", got)
	}

	reopened := Open(dir, time.Minute, 5, 1024, false, 1)
	if got := len(reopened.LogSince(0)); got != 500 {
		t.Fatalf("after reopen from WAL: want 500 durable entries, got %d", got)
	}
}
