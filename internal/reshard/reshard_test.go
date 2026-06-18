package reshard

import "testing"

func TestFreezeAndThaw(t *testing.T) {
	s := &State{}
	if err := s.Freeze("sess-1"); err != nil {
		t.Fatal(err)
	}
	if s.StatusString() != "frozen" {
		t.Fatalf("expected frozen, got %s", s.StatusString())
	}
	if s.Check() != ErrFrozen {
		t.Fatal("expected ErrFrozen from Check")
	}
	if err := s.Thaw("sess-1"); err != nil {
		t.Fatal(err)
	}
	if s.StatusString() != "normal" {
		t.Fatalf("expected normal after thaw, got %s", s.StatusString())
	}
}

func TestThawSessionMismatch(t *testing.T) {
	s := &State{}
	s.Freeze("sess-1")
	if err := s.Thaw("sess-wrong"); err == nil {
		t.Fatal("expected error on session mismatch")
	}
	if s.StatusString() != "frozen" {
		t.Fatal("state should remain frozen after bad thaw")
	}
}

func TestDoubleFreezeRejected(t *testing.T) {
	s := &State{}
	s.Freeze("sess-1")
	if err := s.Freeze("sess-2"); err == nil {
		t.Fatal("expected error on double freeze")
	}
}

func TestLoadCycle(t *testing.T) {
	s := &State{}
	if err := s.StartLoad("sess-1"); err != nil {
		t.Fatal(err)
	}
	if !s.IsLoading() {
		t.Fatal("expected IsLoading true during load")
	}
	if s.Check() != nil {
		t.Fatalf("Check should not block during Loading (buffering), got %v", s.Check())
	}
	bw := BufferedWrite{ID: "e1", Key: "k1", Value: []byte(`{}`)}
	s.Buffer(bw)
	drained := s.DrainBuffer()
	if len(drained) != 1 || drained[0].ID != "e1" {
		t.Fatalf("expected 1 buffered write, got %v", drained)
	}
	s.FinishLoad()
	if s.IsLoading() {
		t.Fatal("expected IsLoading false after finish")
	}
	if s.Check() != nil {
		t.Fatalf("expected nil after load, got %v", s.Check())
	}
}
