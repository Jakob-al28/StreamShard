package epoch

import (
	"encoding/binary"
	"os"
	"sync"
)

type Fencer struct {
	mu   sync.Mutex
	seen uint64
	path string
}

func Load(path string) (*Fencer, error) {
	f := &Fencer{path: path}
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return f, nil
	}
	if err != nil {
		return nil, err
	}
	if len(data) >= 8 {
		f.seen = binary.BigEndian.Uint64(data[:8])
	}
	return f, nil
}

func (f *Fencer) Check(e uint64) (ok bool, current uint64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if e < f.seen {
		return false, f.seen
	}
	if e > f.seen {
		f.seen = e
		f.persist()
	}
	return true, f.seen
}

func (f *Fencer) Seen() uint64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.seen
}

func (f *Fencer) persist() {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], f.seen)
	tmp := f.path + ".tmp"
	if err := os.WriteFile(tmp, buf[:], 0o600); err != nil {
		return
	}
	fh, err := os.Open(tmp)
	if err != nil {
		return
	}
	fh.Sync()
	fh.Close()
	os.Rename(tmp, f.path)
}
