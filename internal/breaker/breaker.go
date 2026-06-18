package breaker

import (
	"sync"
	"time"
)

type state int

const (
	closed   state = iota
	open
	halfOpen
)

type Breaker struct {
	mu           sync.Mutex
	state        state
	failures     int
	threshold    int
	cooldown     time.Duration
	openedAt     time.Time
}

func New(threshold int, cooldown time.Duration) *Breaker {
	return &Breaker{threshold: threshold, cooldown: cooldown}
}

func (b *Breaker) Allow() bool {
	b.mu.Lock()
	defer b.mu.Unlock()

	switch b.state {
	case closed:
		return true
	case open:
		if time.Since(b.openedAt) >= b.cooldown {
			b.state = halfOpen
			return true
		}
		return false
	case halfOpen:
		return false
	}
	return false
}

func (b *Breaker) Success() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.failures = 0
	b.state = closed
}

func (b *Breaker) Failure() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.failures++
	if b.state == halfOpen || b.failures >= b.threshold {
		b.state = open
		b.openedAt = time.Now()
		b.failures = 0
	}
}

func (b *Breaker) State() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	switch b.state {
	case open:
		return "open"
	case halfOpen:
		return "half-open"
	default:
		return "closed"
	}
}
