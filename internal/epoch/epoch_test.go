package epoch

import (
	"os"
	"path/filepath"
	"testing"
)

func tmpPath(t *testing.T) string {
	t.Helper()
	return filepath.Join(t.TempDir(), "epoch")
}

func TestFreshFencerAcceptsAny(t *testing.T) {
	f, _ := Load(tmpPath(t))
	ok, _ := f.Check(0)
	if !ok {
		t.Fatal("fresh fencer should accept epoch 0")
	}
	ok, _ = f.Check(5)
	if !ok {
		t.Fatal("fresh fencer should accept epoch 5")
	}
}

func TestRejectsStalEpoch(t *testing.T) {
	f, _ := Load(tmpPath(t))
	f.Check(10)
	ok, seen := f.Check(9)
	if ok {
		t.Fatal("epoch 9 < seen 10 should be rejected")
	}
	if seen != 10 {
		t.Fatalf("expected seen=10, got %d", seen)
	}
}

func TestAcceptsSameEpoch(t *testing.T) {
	f, _ := Load(tmpPath(t))
	f.Check(5)
	ok, _ := f.Check(5)
	if !ok {
		t.Fatal("same epoch should be accepted (not stale)")
	}
}

func TestAdvancesMonotonically(t *testing.T) {
	f, _ := Load(tmpPath(t))
	for _, e := range []uint64{1, 3, 3, 7, 7, 10} {
		ok, _ := f.Check(e)
		if !ok {
			t.Fatalf("epoch %d should be accepted", e)
		}
	}
	if f.Seen() != 10 {
		t.Fatalf("expected seen=10, got %d", f.Seen())
	}
}

func TestDurableAcrossRestart(t *testing.T) {
	p := tmpPath(t)
	f, _ := Load(p)
	f.Check(42)

	f2, err := Load(p)
	if err != nil {
		t.Fatal(err)
	}
	if f2.Seen() != 42 {
		t.Fatalf("expected seen=42 after reload, got %d", f2.Seen())
	}

	ok, _ := f2.Check(41)
	if ok {
		t.Fatal("reloaded fencer should reject stale epoch 41")
	}
}

func TestMissingFileIsNotError(t *testing.T) {
	_, err := Load("/nonexistent/path/epoch")
	if err != nil && !os.IsNotExist(err) {
		t.Fatalf("unexpected error for missing parent dir: %v", err)
	}
}
