package reshard

import (
	"errors"
	"sync"
)

type Status int

const (
	Normal  Status = iota
	Frozen         // writes return 503
	Loading        // buffering writes while pulling log
)

var (
	ErrFrozen = errors.New("partition frozen for reshard")
)

type BufferedWrite struct {
	ID    string
	Key   string
	Value []byte
}

type State struct {
	mu        sync.RWMutex
	status    Status
	sessionID string
	buffer    []BufferedWrite
}

func (s *State) Freeze(sessionID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.status != Normal {
		return errors.New("already in transition")
	}
	s.status = Frozen
	s.sessionID = sessionID
	return nil
}

func (s *State) Thaw(sessionID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.sessionID != sessionID {
		return errors.New("session mismatch")
	}
	s.status = Normal
	s.sessionID = ""
	return nil
}

func (s *State) StartLoad(sessionID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.status != Normal {
		return errors.New("already in transition")
	}
	s.status = Loading
	s.sessionID = sessionID
	s.buffer = s.buffer[:0]
	return nil
}

func (s *State) Buffer(w BufferedWrite) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.status == Loading {
		s.buffer = append(s.buffer, w)
	}
}

func (s *State) DrainBuffer() []BufferedWrite {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := s.buffer
	s.buffer = nil
	return out
}

func (s *State) FinishLoad() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.status = Normal
	s.sessionID = ""
}

func (s *State) Check() error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.status == Frozen {
		return ErrFrozen
	}
	return nil
}

func (s *State) IsLoading() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.status == Loading
}

func (s *State) StatusString() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	switch s.status {
	case Frozen:
		return "frozen"
	case Loading:
		return "loading"
	default:
		return "normal"
	}
}
