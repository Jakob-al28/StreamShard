package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jakob-al28/StreamShard/internal/epoch"
	ilog "github.com/jakob-al28/StreamShard/internal/log"
	"github.com/jakob-al28/StreamShard/internal/membership"
	"github.com/jakob-al28/StreamShard/internal/partition"
	"github.com/jakob-al28/StreamShard/internal/reshard"
	"github.com/jakob-al28/StreamShard/internal/ring"
)

var (
	p            *partition.Partition
	pmu          sync.RWMutex
	fencer       *epoch.Fencer
	rs           reshard.State
	nodeWindow   time.Duration
	nodeTopK     int
	nodeQueue    int
	nodeDataDir  string
	noIdempotent bool
	walBatch     int

	selfAddr    string
	nodeRing    *ring.Ring
	nodeRF      int
	nodeW       int
	primaryRepl bool
	replClient  *http.Client
	swimMem     *membership.Member
)

func getPartition() *partition.Partition {
	pmu.RLock()
	defer pmu.RUnlock()
	return p
}

func setPartition(newP *partition.Partition) {
	pmu.Lock()
	defer pmu.Unlock()
	p = newP
}

func main() {
	addr := flag.String("addr", ":8080", "listen address")
	window := flag.Duration("window", time.Minute, "aggregate window")
	topK := flag.Int("topk", 10, "top-K entries to track")
	queueCap := flag.Int("queue-cap", 256, "apply queue capacity")
	dataDir := flag.String("data-dir", ".", "directory for durable state")
	swimAddr := flag.String("swim-addr", "", "UDP address for SWIM membership (e.g. 127.0.0.1:9081)")
	swimSeeds := flag.String("swim-seeds", "", "comma-separated SWIM addresses of seed members")
	swimHTTPAddr := flag.String("swim-http-addr", "", "HTTP address to advertise via SWIM (defaults to --addr)")
	peers := flag.String("peers", "", "comma-separated node HTTP addresses (required for --primary-replication)")
	rf := flag.Int("rf", 1, "replication factor")
	w := flag.Int("w", 1, "write quorum")
	primaryReplication := flag.Bool("primary-replication", false, "primary node fans out replication to other replicas instead of the gateway")
	noIdempotentFlag := flag.Bool("no-idempotent", false, "disable idempotency dedup (for benchmarking)")
	walBatchFlag := flag.Int("wal-batch", 1, "max writes coalesced into one WAL write syscall (1 = off; >1 enables batching)")
	flag.Parse()

	nodeWindow = *window
	nodeTopK = *topK
	nodeQueue = *queueCap
	nodeDataDir = *dataDir
	noIdempotent = *noIdempotentFlag
	walBatch = *walBatchFlag

	nodeRF = *rf
	nodeW = *w
	primaryRepl = *primaryReplication
	selfAddr = *swimHTTPAddr
	if primaryRepl {
		if *peers == "" {
			log.Fatal("--peers required when --primary-replication is set")
		}
		nodeRing = ring.New(strings.Split(*peers, ","))
		replClient = &http.Client{
			Timeout: 4 * time.Second,
			Transport: &http.Transport{
				MaxIdleConns:        1000,
				MaxIdleConnsPerHost: 256,
				IdleConnTimeout:     90 * time.Second,
			},
		}
	}

	if err := os.MkdirAll(*dataDir, 0o755); err != nil {
		log.Fatal(err)
	}

	var err error
	fencer, err = epoch.Load(filepath.Join(*dataDir, "epoch"))
	if err != nil {
		log.Fatal(err)
	}

	setPartition(partition.Open(*dataDir, *window, *topK, *queueCap, noIdempotent, walBatch))

	if *swimAddr != "" {
		seeds := strings.Split(*swimSeeds, ",")
		if *swimSeeds == "" {
			seeds = nil
		}
		httpAdvert := *swimHTTPAddr
		if httpAdvert == "" {
			httpAdvert = *addr
		}
		selfAddr = httpAdvert
		swimMem, err = membership.New(*swimAddr, httpAdvert, seeds)
		if err != nil {
			log.Fatalf("swim: %v", err)
		}
	}

	mux := http.NewServeMux()
	mux.HandleFunc("POST /events", handleEvent)
	mux.HandleFunc("POST /replicate", handleReplicate)
	mux.HandleFunc("GET /aggregates", handleAggregates)
	mux.HandleFunc("GET /log", handleLog)
	mux.HandleFunc("GET /snapshot", handleSnapshot)
	mux.HandleFunc("GET /health", handleHealth)
	mux.HandleFunc("POST /reshard/freeze", handleReshardFreeze)
	mux.HandleFunc("POST /reshard/thaw", handleReshardThaw)
	mux.HandleFunc("POST /reshard/load", handleReshardLoad)
	mux.HandleFunc("POST /reshard/abort", handleReshardAbort)

	log.Printf("node listening on %s  queue-cap=%d  epoch=%d",
		*addr, *queueCap, fencer.Seen())
	if err := http.ListenAndServe(*addr, mux); err != nil {
		log.Fatal(err)
	}
}

type eventRequest struct {
	ID    string          `json:"id"`
	Key   string          `json:"key"`
	Value json.RawMessage `json:"value"`
}

func parseEpoch(r *http.Request) uint64 {
	v, _ := strconv.ParseUint(r.Header.Get("X-Epoch"), 10, 64)
	return v
}

func checkEpoch(w http.ResponseWriter, r *http.Request) bool {
	e := parseEpoch(r)
	ok, seen := fencer.Check(e)
	if !ok {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusConflict)
		json.NewEncoder(w).Encode(map[string]any{
			"error": "stale epoch",
			"sent":  e,
			"seen":  seen,
		})
		return false
	}
	return true
}

func checkShedding(w http.ResponseWriter) bool {
	cur := getPartition()
	depth := cur.QueueDepth()
	if depth >= cur.QueueCap() {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Retry-After", "1")
		w.WriteHeader(http.StatusTooManyRequests)
		json.NewEncoder(w).Encode(map[string]any{
			"error": "overloaded",
			"depth": depth,
			"cap":   cur.QueueCap(),
		})
		return true
	}
	return false
}

func handleEvent(w http.ResponseWriter, r *http.Request) {
	if !checkEpoch(w, r) {
		return
	}
	if checkShedding(w) {
		return
	}

	var req eventRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	if req.ID == "" || req.Key == "" {
		http.Error(w, "id and key required", http.StatusBadRequest)
		return
	}

	if err := rs.Check(); err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}
	if rs.IsLoading() {
		rs.Buffer(reshard.BufferedWrite{ID: req.ID, Key: req.Key, Value: req.Value})
		w.WriteHeader(http.StatusAccepted)
		json.NewEncoder(w).Encode(map[string]any{"buffered": true})
		return
	}

	result := getPartition().Apply(req.ID, req.Key, req.Value)
	if !result.Fresh {
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]any{"duplicate": true, "offset": result.Entry.Offset})
		return
	}

	// Move replication responsibility from gateway to primary node.
	if primaryRepl {
		body, _ := json.Marshal(req)
		if ok := fanOutReplicate(req.Key, body, parseEpoch(r)); !ok {
			w.Header().Set("Retry-After", "1")
			http.Error(w, fmt.Sprintf("quorum not reached (need %d)", nodeW), http.StatusServiceUnavailable)
			return
		}
	}

	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(map[string]any{"offset": result.Entry.Offset})
}

// blocks until W copies exist (primary's own apply is 1, so W-1 acks), then returns
// while the rest finish in the background so all RF copies still land
func fanOutReplicate(key string, body []byte, ep uint64) bool {
	replicas := nodeRing.Replicas(key, nodeRF)
	var live map[string]bool
	if swimMem != nil {
		live = swimMem.LiveHTTPAddrs()
	}

	ch := make(chan bool, len(replicas))
	sent := 0
	for _, addr := range replicas {
		if addr == selfAddr {
			continue
		}
		if live != nil && !live[addr] {
			continue
		}
		sent++
		go func(addr string) {
			rctx, rcancel := context.WithTimeout(context.Background(), 4*time.Second)
			defer rcancel()
			ch <- postReplicate(rctx, addr, body, ep)
		}(addr)
	}

	need := nodeW - 1
	acks := 0
	for i := 0; i < sent; i++ {
		if <-ch {
			acks++
		}
		if acks >= need {
			return true
		}
	}
	return acks >= need
}

func postReplicate(ctx context.Context, addr string, body []byte, ep uint64) bool {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, fmt.Sprintf("http://%s/replicate", addr), bytes.NewReader(body))
	if err != nil {
		return false
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Epoch", strconv.FormatUint(ep, 10))
	resp, err := replClient.Do(req)
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)
	return resp.StatusCode == http.StatusNoContent || resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusCreated || resp.StatusCode == http.StatusAccepted
}

func handleReplicate(w http.ResponseWriter, r *http.Request) {
	if !checkEpoch(w, r) {
		return
	}
	if checkShedding(w) {
		return
	}

	var req eventRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	if req.ID == "" || req.Key == "" {
		http.Error(w, "id and key required", http.StatusBadRequest)
		return
	}

	if err := rs.Check(); err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}
	if rs.IsLoading() {
		rs.Buffer(reshard.BufferedWrite{ID: req.ID, Key: req.Key, Value: req.Value})
		w.WriteHeader(http.StatusNoContent)
		return
	}

	getPartition().Apply(req.ID, req.Key, req.Value)
	w.WriteHeader(http.StatusNoContent)
}

func handleAggregates(w http.ResponseWriter, r *http.Request) {
	result := getPartition().Query()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(result)
}

func handleSnapshot(w http.ResponseWriter, r *http.Request) {
	if nodeDataDir == "" {
		http.Error(w, "no data dir", http.StatusNotFound)
		return
	}
	snap, err := ilog.LoadSnapshot(nodeDataDir)
	if err != nil {
		http.Error(w, "no snapshot", http.StatusNotFound)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(snap)
}

func handleLog(w http.ResponseWriter, r *http.Request) {
	cur := getPartition()
	offset := cur.BaseOffset()
	if s := r.URL.Query().Get("from"); s != "" {
		v, err := strconv.ParseUint(s, 10, 64)
		if err != nil {
			http.Error(w, "invalid from", http.StatusBadRequest)
			return
		}
		offset = v
	}
	entries := cur.LogSince(offset)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(entries)
}

func handleHealth(w http.ResponseWriter, r *http.Request) {
	cur := getPartition()
	depth := cur.QueueDepth()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{
		"depth":      depth,
		"cap":        cur.QueueCap(),
		"overloaded": depth >= cur.QueueCap(),
		"epoch":      fencer.Seen(),
		"reshard":    rs.StatusString(),
	})
}

func handleReshardFreeze(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Session string `json:"session"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.Session == "" {
		http.Error(w, "session required", http.StatusBadRequest)
		return
	}
	if err := rs.Freeze(body.Session); err != nil {
		http.Error(w, err.Error(), http.StatusConflict)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{"status": "frozen"})
}

func handleReshardAbort(w http.ResponseWriter, r *http.Request) {
	rs.FinishLoad()
	w.WriteHeader(http.StatusNoContent)
}

func handleReshardThaw(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Session string `json:"session"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.Session == "" {
		http.Error(w, "session required", http.StatusBadRequest)
		return
	}
	if err := rs.Thaw(body.Session); err != nil {
		http.Error(w, err.Error(), http.StatusConflict)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

type loadRequest struct {
	Session string `json:"session"`
	Source  string `json:"source"`
	Epoch   uint64 `json:"epoch"`
}

func handleReshardLoad(w http.ResponseWriter, r *http.Request) {
	var req loadRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	if req.Session == "" || req.Source == "" {
		http.Error(w, "session and source required", http.StatusBadRequest)
		return
	}

	if err := rs.StartLoad(req.Session); err != nil {
		http.Error(w, err.Error(), http.StatusConflict)
		return
	}

	go func() {
		if err := loadFromSource(req.Source, req.Epoch); err != nil {
			log.Printf("reshard load failed: %v", err)
			rs.FinishLoad()
			return
		}
		buffered := rs.DrainBuffer()
		rs.FinishLoad()
		cur := getPartition()
		for _, bw := range buffered {
			cur.Apply(bw.ID, bw.Key, bw.Value)
		}
		log.Printf("reshard load complete: replayed %d buffered writes", len(buffered))
	}()

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{"status": "loading"})
}

func loadFromSource(sourceAddr string, newEpoch uint64) error {
	var baseOffset uint64

	snapResp, err := http.Get(fmt.Sprintf("http://%s/snapshot", sourceAddr))
	if err == nil && snapResp.StatusCode == http.StatusOK {
		var snap ilog.Snapshot
		if json.NewDecoder(snapResp.Body).Decode(&snap) == nil {
			baseOffset = snap.BaseOffset
			if nodeDataDir != "" {
				os.Remove(filepath.Join(nodeDataDir, "wal"))
				os.Remove(filepath.Join(nodeDataDir, "snapshot.json"))
				data, _ := json.Marshal(snap)
				tmp := filepath.Join(nodeDataDir, "snapshot.json.tmp")
				os.WriteFile(tmp, data, 0o644)
				os.Rename(tmp, filepath.Join(nodeDataDir, "snapshot.json"))
			}
		}
		snapResp.Body.Close()
	}

	tailResp, err := http.Get(fmt.Sprintf("http://%s/log?from=%d", sourceAddr, baseOffset))
	if err != nil {
		return err
	}
	defer tailResp.Body.Close()

	body, err := io.ReadAll(tailResp.Body)
	if err != nil {
		return err
	}

	var entries []ilog.Entry
	if err := json.Unmarshal(body, &entries); err != nil {
		return err
	}

	if nodeDataDir != "" {
		os.Remove(filepath.Join(nodeDataDir, "wal"))
	}
	newP := partition.Open(nodeDataDir, nodeWindow, nodeTopK, nodeQueue, noIdempotent, walBatch)
	for _, e := range entries {
		newP.Apply(e.ID, e.Key, e.Value)
	}

	setPartition(newP)

	if newEpoch > 0 {
		fencer.Check(newEpoch)
	}

	log.Printf("reshard load complete: snapshot_base=%d tail=%d entries, epoch=%d", baseOffset, len(entries), fencer.Seen())
	return nil
}
