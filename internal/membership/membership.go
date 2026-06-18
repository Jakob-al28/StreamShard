package membership

import (
	"encoding/json"
	"log"
	"math/rand"
	"net"
	"sync"
	"time"
)

type State int

const (
	Alive State = iota
	Suspect
	Dead
)

const (
	pingInterval   = 2 * time.Second
	pingTimeout    = 1 * time.Second
	suspectTimeout = 10 * time.Second
	eventTTL       = 10
)

type EventType int

const (
	EventJoin EventType = iota
	EventDead
)

type Event struct {
	Type     EventType
	Addr     string
	HTTPAddr string
}

type member struct {
	addr      string
	httpAddr  string
	state     State
	suspectAt time.Time
}

type msgType string

const (
	msgPing    msgType = "ping"
	msgAck     msgType = "ack"
	msgPingReq msgType = "ping-req"
)

type gossipEvent struct {
	Type     string `json:"type"`
	Addr     string `json:"addr"`
	HTTPAddr string `json:"http_addr,omitempty"`
	TTL      int    `json:"ttl"`
}

type message struct {
	Type   msgType       `json:"type"`
	From   string        `json:"from"`
	Target string        `json:"target,omitempty"`
	Events []gossipEvent `json:"events,omitempty"`
}

type Member struct {
	addr     string
	httpAddr string
	conn     *net.UDPConn
	mu       sync.Mutex
	members  map[string]*member
	pending  []gossipEvent
	events   chan Event
	ackMu    sync.Mutex
	ackWait  map[string]chan struct{}
}

func New(addr, httpAddr string, seeds []string) (*Member, error) {
	udpAddr, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		return nil, err
	}
	conn, err := net.ListenUDP("udp", udpAddr)
	if err != nil {
		return nil, err
	}

	m := &Member{
		addr:     addr,
		httpAddr: httpAddr,
		conn:     conn,
		members:  make(map[string]*member),
		events:   make(chan Event, 64),
		ackWait:  make(map[string]chan struct{}),
	}

	m.mu.Lock()
	m.members[addr] = &member{addr: addr, httpAddr: httpAddr, state: Alive}
	for _, seed := range seeds {
		if seed != addr {
			m.members[seed] = &member{addr: seed, state: Alive}
		}
	}
	m.mu.Unlock()

	go m.recv()
	go m.probe()
	go m.checkSuspects()

	joinEvent := gossipEvent{Type: "join", Addr: addr, HTTPAddr: httpAddr, TTL: eventTTL}
	for _, seed := range seeds {
		if seed != addr {
			m.sendMsg(seed, message{Type: msgPing, From: addr, Events: []gossipEvent{joinEvent}})
		}
	}

	log.Printf("swim: member started on %s http=%s seeds=%v", addr, httpAddr, seeds)
	return m, nil
}

func (m *Member) Events() <-chan Event {
	return m.events
}

func (m *Member) Members() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]string, 0, len(m.members))
	for addr, mem := range m.members {
		if mem.state != Dead {
			out = append(out, addr)
		}
	}
	return out
}

// returns the set of HTTP addresses for non-dead members
// Used by primary replication to skip replicas SWIM believes are down
func (m *Member) LiveHTTPAddrs() map[string]bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make(map[string]bool, len(m.members))
	for _, mem := range m.members {
		if mem.state != Dead && mem.httpAddr != "" {
			out[mem.httpAddr] = true
		}
	}
	return out
}

func (m *Member) recv() {
	buf := make([]byte, 4096)
	for {
		n, src, err := m.conn.ReadFromUDP(buf)
		if err != nil {
			return
		}
		var msg message
		if err := json.Unmarshal(buf[:n], &msg); err != nil {
			log.Printf("swim: bad message from %s: %v", src, err)
			continue
		}
		if msg.Type == msgAck {
			msg.From = src.String()
		}
		m.handleMsg(msg)
	}
}

func (m *Member) handleMsg(msg message) {
	m.mu.Lock()
	m.mergeEvents(msg.Events)
	m.mu.Unlock()

	switch msg.Type {
	case msgPing:
		m.sendMsg(msg.From, message{
			Type:   msgAck,
			From:   m.addr,
			Events: m.drainEvents(),
		})

	case msgAck:
		m.mu.Lock()
		if mem, ok := m.members[msg.From]; ok && mem.state == Suspect {
			mem.state = Alive
			log.Printf("swim: %s refuted suspicion", msg.From)
		}
		m.mu.Unlock()
		m.ackMu.Lock()
		if ch, ok := m.ackWait[resolveAddr(msg.From)]; ok {
			select {
			case ch <- struct{}{}:
			default:
			}
		}
		m.ackMu.Unlock()

	case msgPingReq:
		go func() {
			if m.directPing(msg.Target) {
				m.sendMsg(msg.From, message{Type: msgAck, From: msg.Target})
			}
		}()
	}
}

func (m *Member) mergeEvents(events []gossipEvent) {
	for _, e := range events {
		if e.Addr == "" || e.TTL <= 0 {
			continue
		}
		switch e.Type {
		case "join":
			if _, exists := m.members[e.Addr]; !exists {
				m.members[e.Addr] = &member{addr: e.Addr, httpAddr: e.HTTPAddr, state: Alive}
				log.Printf("swim: learned about new member %s http=%s", e.Addr, e.HTTPAddr)
				select {
				case m.events <- Event{Type: EventJoin, Addr: e.Addr, HTTPAddr: e.HTTPAddr}:
				default:
				}
			} else if m.members[e.Addr].state == Dead {
				m.members[e.Addr].state = Alive
				m.members[e.Addr].httpAddr = e.HTTPAddr
				select {
				case m.events <- Event{Type: EventJoin, Addr: e.Addr, HTTPAddr: e.HTTPAddr}:
				default:
				}
			}
			m.pending = append(m.pending, gossipEvent{Type: "join", Addr: e.Addr, HTTPAddr: e.HTTPAddr, TTL: e.TTL - 1})

		case "dead":
			mem, exists := m.members[e.Addr]
			if !exists || mem.state == Dead {
				continue
			}
			mem.state = Dead
			log.Printf("swim: member %s http=%s declared dead via gossip", e.Addr, mem.httpAddr)
			select {
			case m.events <- Event{Type: EventDead, Addr: e.Addr, HTTPAddr: mem.httpAddr}:
			default:
			}
			m.pending = append(m.pending, gossipEvent{Type: "dead", Addr: e.Addr, HTTPAddr: mem.httpAddr, TTL: e.TTL - 1})
		}
	}
}

func (m *Member) drainEvents() []gossipEvent {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.pending) == 0 {
		return nil
	}
	n := 4
	if len(m.pending) < n {
		n = len(m.pending)
	}
	out := m.pending[:n]
	m.pending = m.pending[n:]
	return out
}

func (m *Member) probe() {
	t := time.NewTicker(pingInterval)
	defer t.Stop()
	for range t.C {
		target := m.pickRandom(m.addr)
		if target == "" {
			continue
		}
		if m.directPing(target) {
			continue
		}
		helpers := m.pickN(2, m.addr, target)
		key := resolveAddr(target)
		ch := make(chan struct{}, 1)
		m.ackMu.Lock()
		m.ackWait[key] = ch
		m.ackMu.Unlock()
		for _, h := range helpers {
			m.sendMsg(h, message{Type: msgPingReq, From: m.addr, Target: target})
		}
		confirmed := false
		select {
		case <-ch:
			confirmed = true
		case <-time.After(pingTimeout * 5):
		}
		m.ackMu.Lock()
		delete(m.ackWait, key)
		m.ackMu.Unlock()
		if confirmed {
			continue
		}
		m.mu.Lock()
		if mem, ok := m.members[target]; ok && mem.state == Alive {
			mem.state = Suspect
			mem.suspectAt = time.Now()
			log.Printf("swim: %s suspected unreachable", target)
		}
		m.mu.Unlock()
	}
}

func (m *Member) checkSuspects() {
	t := time.NewTicker(500 * time.Millisecond)
	defer t.Stop()
	for range t.C {
		m.mu.Lock()
		for _, mem := range m.members {
			if mem.state == Suspect && time.Since(mem.suspectAt) > suspectTimeout {
				mem.state = Dead
				log.Printf("swim: %s http=%s declared dead after suspicion timeout", mem.addr, mem.httpAddr)
				m.pending = append(m.pending, gossipEvent{Type: "dead", Addr: mem.addr, HTTPAddr: mem.httpAddr, TTL: eventTTL})
				select {
				case m.events <- Event{Type: EventDead, Addr: mem.addr, HTTPAddr: mem.httpAddr}:
				default:
				}
			}
		}
		m.mu.Unlock()
	}
}

func resolveAddr(addr string) string {
	u, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		return addr
	}
	return u.String()
}

func (m *Member) directPing(target string) bool {
	key := resolveAddr(target)
	ch := make(chan struct{}, 1)
	m.ackMu.Lock()
	m.ackWait[key] = ch
	m.ackMu.Unlock()
	defer func() {
		m.ackMu.Lock()
		delete(m.ackWait, key)
		m.ackMu.Unlock()
	}()

	m.sendMsg(target, message{Type: msgPing, From: m.addr, Events: m.drainEvents()})

	select {
	case <-ch:
		return true
	case <-time.After(pingTimeout):
		return false
	}
}

func (m *Member) sendMsg(addr string, msg message) {
	data, err := json.Marshal(msg)
	if err != nil {
		return
	}
	udpAddr, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		return
	}
	m.conn.WriteToUDP(data, udpAddr)
}

func (m *Member) pickRandom(exclude ...string) string {
	m.mu.Lock()
	defer m.mu.Unlock()
	excl := make(map[string]bool)
	for _, e := range exclude {
		excl[e] = true
	}
	candidates := make([]string, 0, len(m.members))
	for addr, mem := range m.members {
		if !excl[addr] && mem.state != Dead {
			candidates = append(candidates, addr)
		}
	}
	if len(candidates) == 0 {
		return ""
	}
	return candidates[rand.Intn(len(candidates))]
}

func (m *Member) pickN(n int, exclude ...string) []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	excl := make(map[string]bool)
	for _, e := range exclude {
		excl[e] = true
	}
	candidates := make([]string, 0, len(m.members))
	for addr, mem := range m.members {
		if !excl[addr] && mem.state != Dead {
			candidates = append(candidates, addr)
		}
	}
	rand.Shuffle(len(candidates), func(i, j int) { candidates[i], candidates[j] = candidates[j], candidates[i] })
	if n > len(candidates) {
		n = len(candidates)
	}
	return candidates[:n]
}
