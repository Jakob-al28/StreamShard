package breaker

import (
	"testing"
	"time"
)

func TestClosedAllows(t *testing.T) {
	b := New(3, time.Second)
	if !b.Allow() {
		t.Fatal("closed breaker should allow")
	}
}

func TestOpensAfterThreshold(t *testing.T) {
	b := New(3, time.Second)
	b.Failure()
	b.Failure()
	if b.State() != "closed" {
		t.Fatal("should still be closed at 2 failures")
	}
	b.Failure()
	if b.State() != "open" {
		t.Fatalf("expected open after %d failures, got %s", 3, b.State())
	}
	if b.Allow() {
		t.Fatal("open breaker should deny")
	}
}

func TestHalfOpenAfterCooldown(t *testing.T) {
	b := New(1, 20*time.Millisecond)
	b.Failure()
	if b.Allow() {
		t.Fatal("should be denied while open")
	}
	time.Sleep(25 * time.Millisecond)
	if !b.Allow() {
		t.Fatal("should allow probe after cooldown (half-open)")
	}
	if b.State() != "half-open" {
		t.Fatalf("expected half-open, got %s", b.State())
	}
}

func TestClosesOnProbeSuccess(t *testing.T) {
	b := New(1, 20*time.Millisecond)
	b.Failure()
	time.Sleep(25 * time.Millisecond)
	b.Allow()
	b.Success()
	if b.State() != "closed" {
		t.Fatalf("expected closed after probe success, got %s", b.State())
	}
	if !b.Allow() {
		t.Fatal("closed breaker should allow")
	}
}

func TestReopensOnProbeFailure(t *testing.T) {
	b := New(1, 20*time.Millisecond)
	b.Failure()
	time.Sleep(25 * time.Millisecond)
	b.Allow()
	b.Failure()
	if b.State() != "open" {
		t.Fatalf("expected open after probe failure, got %s", b.State())
	}
}

func TestSuccessResetsClosed(t *testing.T) {
	b := New(3, time.Second)
	b.Failure()
	b.Failure()
	b.Success()
	b.Failure()
	if b.State() != "closed" {
		t.Fatal("success should reset failure count; one failure after reset should not open")
	}
}
