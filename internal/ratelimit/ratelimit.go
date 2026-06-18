package ratelimit

import (
	"sync"
	"time"
)

type Bucket struct {
	mu       sync.Mutex
	tokens   float64
	rate     float64
	burst    float64
	lastTime time.Time
}

func New(rate float64, burst int) *Bucket {
	return &Bucket{
		tokens:   float64(burst),
		rate:     rate,
		burst:    float64(burst),
		lastTime: time.Now(),
	}
}

func (b *Bucket) Allow() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	now := time.Now()
	b.tokens = min(b.burst, b.tokens+now.Sub(b.lastTime).Seconds()*b.rate)
	b.lastTime = now
	if b.tokens < 1 {
		return false
	}
	b.tokens--
	return true
}

func (b *Bucket) full() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	now := time.Now()
	b.tokens = min(b.burst, b.tokens+now.Sub(b.lastTime).Seconds()*b.rate)
	b.lastTime = now
	return b.tokens >= b.burst
}

type Map struct {
	mu    sync.RWMutex
	rate  float64
	burst int
	m     map[string]*Bucket
}

func NewMap(rate float64, burst int) *Map {
	m := &Map{rate: rate, burst: burst, m: make(map[string]*Bucket)}
	go m.sweep()
	return m
}

func (m *Map) Allow(key string) bool {
	m.mu.RLock()
	b, ok := m.m[key]
	m.mu.RUnlock()
	if !ok {
		m.mu.Lock()
		if b, ok = m.m[key]; !ok {
			b = New(m.rate, m.burst)
			m.m[key] = b
		}
		m.mu.Unlock()
	}
	return b.Allow()
}

func (m *Map) sweep() {
	t := time.NewTicker(time.Minute)
	defer t.Stop()
	for range t.C {
		m.mu.Lock()
		for k, b := range m.m {
			if b.full() {
				delete(m.m, k)
			}
		}
		m.mu.Unlock()
	}
}
