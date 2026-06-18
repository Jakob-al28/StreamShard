package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"sync"
	"time"
)

type epochStore struct {
	mu     sync.RWMutex
	epochs map[string]uint64
	path   string
}

func loadEpochStore(path string) (*epochStore, error) {
	s := &epochStore{epochs: make(map[string]uint64), path: path}
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return s, nil
		}
		return nil, err
	}
	if err := json.Unmarshal(data, &s.epochs); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *epochStore) persist() {
	if s.path == "" {
		return
	}
	data, err := json.Marshal(s.epochs)
	if err != nil {
		log.Printf("epoch persist marshal: %v", err)
		return
	}
	tmp := s.path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		log.Printf("epoch persist write: %v", err)
		return
	}
	if err := os.Rename(tmp, s.path); err != nil {
		log.Printf("epoch persist rename: %v", err)
	}
}

func (s *epochStore) current(partition string) uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.epochs[partition]
}

func (s *epochStore) bump(partition string) uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.epochs[partition]++
	e := s.epochs[partition]
	s.persist()
	return e
}

var client = &http.Client{Timeout: 10 * time.Second}

func nodePost(addr, path string, body any) error {
	b, err := json.Marshal(body)
	if err != nil {
		return err
	}
	resp, err := client.Post(fmt.Sprintf("http://%s%s", addr, path), "application/json", bytes.NewReader(b))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		return fmt.Errorf("%s %s: status %d", addr, path, resp.StatusCode)
	}
	return nil
}

func main() {
	addr    := flag.String("addr", ":6060", "listen address")
	dataDir := flag.String("data-dir", ".", "directory for durable state")
	flag.Parse()

	if err := os.MkdirAll(*dataDir, 0o755); err != nil {
		log.Fatal(err)
	}
	store, err := loadEpochStore(*dataDir + "/epochs.json")
	if err != nil {
		log.Fatal(err)
	}

	mux := http.NewServeMux()

	mux.HandleFunc("GET /epoch", func(w http.ResponseWriter, r *http.Request) {
		part := r.URL.Query().Get("partition")
		if part == "" {
			part = "_default"
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"partition": part,
			"epoch":     store.current(part),
		})
	})

	mux.HandleFunc("POST /epoch/bump", func(w http.ResponseWriter, r *http.Request) {
		part := r.URL.Query().Get("partition")
		if part == "" {
			part = "_default"
		}
		e := store.bump(part)
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"partition": part,
			"epoch":     e,
		})
	})

	mux.HandleFunc("GET /epochs", func(w http.ResponseWriter, r *http.Request) {
		store.mu.RLock()
		out := make(map[string]uint64, len(store.epochs))
		for k, v := range store.epochs {
			out[k] = v
		}
		store.mu.RUnlock()
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(out)
	})

	// live=false freezes the source for the duration of the transfer; live=true buffers writes on the target instead.
	mux.HandleFunc("POST /reshard", func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Source    string `json:"source"`
			Target    string `json:"target"`
			Partition string `json:"partition"`
			Live      *bool  `json:"live"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		if req.Source == "" || req.Target == "" {
			http.Error(w, "source and target required", http.StatusBadRequest)
			return
		}
		if req.Partition == "" {
			req.Partition = "_default"
		}
		live := req.Live == nil || *req.Live

		session := fmt.Sprintf("reshard-%d", time.Now().UnixNano())

		if !live {
			if err := nodePost(req.Source, "/reshard/freeze", map[string]any{"session": session}); err != nil {
				http.Error(w, fmt.Sprintf("freeze failed: %v", err), http.StatusInternalServerError)
				return
			}
			log.Printf("reshard %s: source %s frozen (synchronous reshard)", session, req.Source)
		}

		newEpoch := store.bump(req.Partition)
		log.Printf("reshard %s: epoch bumped to %d", session, newEpoch)

		if err := nodePost(req.Target, "/reshard/load", map[string]any{
			"session": session,
			"source":  req.Source,
			"epoch":   newEpoch,
		}); err != nil {
			if !live {
				nodePost(req.Source, "/reshard/thaw", map[string]any{"session": session})
			}
			http.Error(w, fmt.Sprintf("load failed: %v", err), http.StatusInternalServerError)
			return
		}
		log.Printf("reshard %s: target %s loading (live=%v)", session, req.Target, live)

		timedOut := true
		deadline := time.Now().Add(30 * time.Second)
		for time.Now().Before(deadline) {
			resp, err := client.Get(fmt.Sprintf("http://%s/health", req.Target))
			if err == nil {
				var h map[string]any
				json.NewDecoder(resp.Body).Decode(&h)
				resp.Body.Close()
				if h["reshard"] == "normal" {
					timedOut = false
					break
				}
			}
			time.Sleep(100 * time.Millisecond)
		}

		if timedOut {
			log.Printf("reshard %s: timed out waiting for target %s", session, req.Target)
			nodePost(req.Target, "/reshard/abort", map[string]any{})
			if !live {
				if err := nodePost(req.Source, "/reshard/thaw", map[string]any{"session": session}); err != nil {
					log.Printf("reshard %s: thaw-on-timeout warning: %v", session, err)
				}
			}
			http.Error(w, "reshard timed out", http.StatusGatewayTimeout)
			return
		}

		if !live {
			if err := nodePost(req.Source, "/reshard/thaw", map[string]any{"session": session}); err != nil {
				log.Printf("reshard %s: thaw warning: %v", session, err)
			}
			log.Printf("reshard %s: source %s thawed", session, req.Source)
		}
		log.Printf("reshard %s: complete", session)

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"session":   session,
			"partition": req.Partition,
			"epoch":     newEpoch,
			"source":    req.Source,
			"target":    req.Target,
			"live":      live,
		})
	})

	// Bumps epoch to fence the dead node so stale writes from it get rejected.
	mux.HandleFunc("POST /failover", func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Dead string `json:"dead"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Dead == "" {
			http.Error(w, "dead required", http.StatusBadRequest)
			return
		}
		newEpoch := store.bump("_default")
		log.Printf("failover: dead=%s epoch bumped to %d", req.Dead, newEpoch)
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"dead":  req.Dead,
			"epoch": newEpoch,
		})
	})

	log.Printf("controlplane listening on %s", *addr)
	if err := http.ListenAndServe(*addr, mux); err != nil {
		log.Fatal(err)
	}
}
