package ratelimit

import (
	"testing"
	"time"
)

func TestBurstAllowed(t *testing.T) {
	b := New(1000, 10)
	for i := range 10 {
		if !b.Allow() {
			t.Fatalf("request %d should be allowed within burst", i)
		}
	}
}

func TestBurstExhausted(t *testing.T) {
	b := New(1, 5)
	for range 5 {
		b.Allow()
	}
	if b.Allow() {
		t.Fatal("bucket should be empty after burst exhausted")
	}
}

func TestRefill(t *testing.T) {
	b := New(1000, 5)
	for range 5 {
		b.Allow()
	}
	if b.Allow() {
		t.Fatal("should be empty")
	}
	time.Sleep(5 * time.Millisecond)
	if !b.Allow() {
		t.Fatal("should have refilled at least one token")
	}
}

func TestConcurrentAllow(t *testing.T) {
	b := New(1, 100)
	allowed := make(chan bool, 200)
	for range 200 {
		go func() { allowed <- b.Allow() }()
	}
	count := 0
	for range 200 {
		if <-allowed {
			count++
		}
	}
	if count != 100 {
		t.Fatalf("expected exactly 100 allowed (burst=100), got %d", count)
	}
}
