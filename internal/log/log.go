package log

import (
	"bufio"
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type Entry struct {
	Offset    uint64
	ID        string
	Key       string
	Value     []byte
	Timestamp time.Time
}

type Log struct {
	entries      []Entry
	baseOffset   uint64
	index        map[string]struct{}
	wal          *os.File
	dir          string
	mu           sync.Mutex
	noIdempotent bool
}

type Snapshot struct {
	BaseOffset uint64            `json:"base_offset"`
	Counts     map[string]uint64 `json:"counts"`
	Timeline   []SnapshotEntry   `json:"timeline"`
}

type SnapshotEntry struct {
	Key string    `json:"key"`
	At  time.Time `json:"at"`
}

func New() *Log {
	l, _ := Open("")
	return l
}

func Open(dir string) (*Log, error) {
	l := &Log{index: make(map[string]struct{}), dir: dir}
	if dir == "" {
		return l, nil
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}

	snap, _ := LoadSnapshot(dir)
	if snap != nil {
		l.baseOffset = snap.BaseOffset
	}

	path := filepath.Join(dir, "wal")
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		var e Entry
		if json.Unmarshal(scanner.Bytes(), &e) == nil {
			if _, dup := l.index[e.ID]; !dup {
				e.Offset = l.baseOffset + uint64(len(l.entries))
				l.entries = append(l.entries, e)
				l.index[e.ID] = struct{}{}
			}
		}
	}
	l.wal = f
	go l.fsyncer()
	return l, nil
}

func LoadSnapshot(dir string) (*Snapshot, error) {
	data, err := os.ReadFile(filepath.Join(dir, "snapshot.json"))
	if err != nil {
		return nil, err
	}
	var s Snapshot
	if err := json.Unmarshal(data, &s); err != nil {
		return nil, err
	}
	return &s, nil
}

func (l *Log) fsyncer() {
	t := time.NewTicker(100 * time.Millisecond)
	defer t.Stop()
	for range t.C {
		l.mu.Lock()
		if l.wal != nil {
			l.wal.Sync()
		}
		l.mu.Unlock()
	}
}

func (l *Log) Append(id, key string, value []byte) (Entry, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if !l.noIdempotent {
		if _, dup := l.index[id]; dup {
			return Entry{}, false
		}
	}
	e := Entry{
		Offset:    l.baseOffset + uint64(len(l.entries)),
		ID:        id,
		Key:       key,
		Value:     value,
		Timestamp: time.Now(),
	}
	l.entries = append(l.entries, e)
	l.index[e.ID] = struct{}{}
	if l.wal != nil {
		b, _ := json.Marshal(e)
		b = append(b, '\n')
		l.wal.Write(b)
	}
	return e, true
}

// AppendBatch appends a batch of records under a single lock and a single WAL
// write syscall, amortising the per-record marshal and write cost
func (l *Log) AppendBatch(ids, keys []string, values [][]byte) ([]Entry, []bool) {
	n := len(ids)
	entries := make([]Entry, n)
	fresh := make([]bool, n)

	l.mu.Lock()
	defer l.mu.Unlock()

	var buf []byte
	now := time.Now()
	for i := 0; i < n; i++ {
		if !l.noIdempotent {
			if _, dup := l.index[ids[i]]; dup {
				continue
			}
		}
		e := Entry{
			Offset:    l.baseOffset + uint64(len(l.entries)),
			ID:        ids[i],
			Key:       keys[i],
			Value:     values[i],
			Timestamp: now,
		}
		l.entries = append(l.entries, e)
		l.index[e.ID] = struct{}{}
		entries[i] = e
		fresh[i] = true
		if l.wal != nil {
			b, _ := json.Marshal(e)
			buf = append(buf, b...)
			buf = append(buf, '\n')
		}
	}
	if l.wal != nil && len(buf) > 0 {
		l.wal.Write(buf)
	}
	return entries, fresh
}

func (l *Log) Since(offset uint64) []Entry {
	l.mu.Lock()
	defer l.mu.Unlock()
	if offset < l.baseOffset {
		offset = l.baseOffset
	}
	idx := offset - l.baseOffset
	if idx >= uint64(len(l.entries)) {
		return nil
	}
	return l.entries[idx:]
}

func (l *Log) Head() uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.baseOffset + uint64(len(l.entries))
}

func (l *Log) BaseOffset() uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.baseOffset
}

func (l *Log) Entries() []Entry {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.entries
}

func (l *Log) SetNoIdempotent(v bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.noIdempotent = v
}

func (l *Log) Compact(snap Snapshot) error {
	if l.dir == "" {
		return nil
	}
	l.mu.Lock()
	defer l.mu.Unlock()

	data, err := json.Marshal(snap)
	if err != nil {
		return err
	}
	tmp := filepath.Join(l.dir, "snapshot.json.tmp")
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	if err := os.Rename(tmp, filepath.Join(l.dir, "snapshot.json")); err != nil {
		return err
	}

	keep := snap.BaseOffset - l.baseOffset
	if keep > uint64(len(l.entries)) {
		keep = uint64(len(l.entries))
	}
	l.entries = l.entries[keep:]
	l.baseOffset = snap.BaseOffset

	if l.wal != nil {
		l.wal.Close()
		newPath := filepath.Join(l.dir, "wal")
		f, err := os.OpenFile(newPath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
		if err != nil {
			return err
		}
		for _, e := range l.entries {
			b, _ := json.Marshal(e)
			b = append(b, '\n')
			f.Write(b)
		}
		f.Sync()
		l.wal = f
	}
	return nil
}
