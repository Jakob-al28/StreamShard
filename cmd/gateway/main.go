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
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jakob-al28/StreamShard/internal/breaker"
	"github.com/jakob-al28/StreamShard/internal/membership"
	"github.com/jakob-al28/StreamShard/internal/ratelimit"
	"github.com/jakob-al28/StreamShard/internal/ring"
)

type gateway struct {
	ring        *ring.Ring
	rf          int
	w           int
	client      *http.Client
	limiter     *ratelimit.Map
	noLimit     bool
	primaryRepl bool
	breakers    map[string]*breaker.Breaker
	bmu         sync.RWMutex
	epoch       atomic.Uint64
	overloaded  sync.Map
	sem         chan struct{}
}

func (gw *gateway) isOverloaded(addr string) bool {
	v, ok := gw.overloaded.Load(addr)
	return ok && v.(bool)
}

func (gw *gateway) pollEpoch(cpAddr string, interval time.Duration) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for range t.C {
		e, err := fetchEpoch(cpAddr)
		if err == nil && e > gw.epoch.Load() {
			gw.epoch.Store(e)
			log.Printf("epoch updated to %d from controlplane", e)
		}
	}
}

func (gw *gateway) pollHealth(interval time.Duration) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for range t.C {
		for _, addr := range gw.ring.Nodes() {
			go func(a string) {
				resp, err := gw.client.Get(fmt.Sprintf("http://%s/health", a))
				if err != nil {
					gw.overloaded.Store(a, false)
					return
				}
				defer resp.Body.Close()
				var h map[string]any
				if json.NewDecoder(resp.Body).Decode(&h) == nil {
					gw.overloaded.Store(a, h["overloaded"] == true)
				}
			}(addr)
		}
	}
}

func main() {
	addr := flag.String("addr", ":7070", "listen address")
	peers := flag.String("peers", "", "comma-separated node addresses")
	rf := flag.Int("rf", 1, "replication factor")
	w := flag.Int("w", 1, "write quorum")
	rate := flag.Float64("rate", 5000, "token bucket refill rate (requests/sec)")
	burst := flag.Int("burst", 1000, "token bucket burst size")
	breakerThresh := flag.Int("breaker-threshold", 5, "failures before circuit opens")
	breakerCooldown := flag.Duration("breaker-cooldown", 10*time.Second, "cooldown before half-open probe")
	healthInterval := flag.Duration("health-interval", time.Second, "node health poll interval")
	controlplane := flag.String("controlplane", "", "control plane address (optional)")
	swimAddr := flag.String("swim-addr", "", "UDP address for SWIM membership (e.g. :7071)")
	swimSeeds := flag.String("swim-seeds", "", "comma-separated SWIM addresses of seed members")
	maxInflight := flag.Int("max-inflight", 1000, "max concurrent event writes; excess requests get 503")
	disableRateLimit := flag.Bool("disable-ratelimit", false, "skip per-key rate limiting entirely")
	primaryReplication := flag.Bool("primary-replication", false, "send only to the primary, which fans out replication itself")
	flag.Parse()

	if *peers == "" {
		log.Fatal("--peers required")
	}
	if *w > *rf {
		log.Fatalf("w (%d) cannot exceed rf (%d)", *w, *rf)
	}

	nodes := strings.Split(*peers, ",")
	if *rf > len(nodes) {
		log.Fatalf("rf (%d) cannot exceed node count (%d)", *rf, len(nodes))
	}
	breakers := make(map[string]*breaker.Breaker, len(nodes))
	for _, n := range nodes {
		breakers[n] = breaker.New(*breakerThresh, *breakerCooldown)
	}

	gw := &gateway{
		ring: ring.New(nodes),
		rf:   *rf,
		w:    *w,
		client: &http.Client{
			Timeout: 5 * time.Second,
			Transport: &http.Transport{
				MaxIdleConns:        1000,
				MaxIdleConnsPerHost: 256,
				IdleConnTimeout:     90 * time.Second,
			},
		},
		limiter:     ratelimit.NewMap(*rate, *burst),
		noLimit:     *disableRateLimit,
		primaryRepl: *primaryReplication,
		breakers:    breakers,
		sem:         make(chan struct{}, *maxInflight),
	}

	go gw.pollHealth(*healthInterval)

	if *controlplane != "" {
		e, err := fetchEpoch(*controlplane)
		if err != nil {
			log.Printf("warning: could not fetch epoch from controlplane: %v", err)
		} else {
			gw.epoch.Store(e)
			log.Printf("fetched epoch %d from controlplane", e)
		}
		go gw.pollEpoch(*controlplane, *healthInterval)
	}

	var swimMem *membership.Member
	if *swimAddr != "" {
		seeds := nodes
		if *swimSeeds != "" {
			seeds = strings.Split(*swimSeeds, ",")
		}
		var err error
		swimMem, err = membership.New(*swimAddr, "", seeds)
		if err != nil {
			log.Fatalf("swim: %v", err)
		}
		go gw.watchMembership(swimMem, *controlplane, *breakerThresh, *breakerCooldown)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("POST /events", gw.handleEvent)
	mux.HandleFunc("GET /aggregates", gw.handleAggregates)
	mux.HandleFunc("GET /ring", gw.handleRing)
	mux.HandleFunc("GET /health", gw.handleHealth)
	mux.HandleFunc("GET /swim", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if swimMem == nil {
			json.NewEncoder(w).Encode(map[string]any{"members": nil})
			return
		}
		json.NewEncoder(w).Encode(map[string]any{"members": swimMem.Members()})
	})

	log.Printf("gateway listening on %s  rf=%d w=%d  rate=%.0f burst=%d  epoch=%d  nodes: %v",
		*addr, *rf, *w, *rate, *burst, gw.epoch.Load(), nodes)
	if err := http.ListenAndServe(*addr, mux); err != nil {
		log.Fatal(err)
	}
}

func (gw *gateway) watchMembership(mem *membership.Member, cpAddr string, breakerThresh int, breakerCooldown time.Duration) {
	for ev := range mem.Events() {
		httpAddr := ev.HTTPAddr
		if httpAddr == "" {
			continue
		}
		switch ev.Type {
		case membership.EventJoin:
			gw.ring.Add(httpAddr)
			gw.bmu.Lock()
			if _, ok := gw.breakers[httpAddr]; !ok {
				gw.breakers[httpAddr] = breaker.New(breakerThresh, breakerCooldown)
			}
			gw.bmu.Unlock()
			log.Printf("swim: node joined %s, added to ring", httpAddr)

		case membership.EventDead:
			gw.ring.Remove(httpAddr)
			gw.overloaded.Delete(httpAddr)
			log.Printf("swim: node dead %s, removed from ring", httpAddr)
			if cpAddr != "" {
				go gw.notifyFailover(cpAddr, httpAddr)
			}
		}
	}
}

func (gw *gateway) notifyFailover(cpAddr, deadAddr string) {
	body, _ := json.Marshal(map[string]string{"dead": deadAddr})
	resp, err := gw.client.Post(
		fmt.Sprintf("http://%s/failover", cpAddr),
		"application/json",
		bytes.NewReader(body),
	)
	if err != nil {
		log.Printf("swim: failover notify failed for %s: %v", deadAddr, err)
		return
	}
	resp.Body.Close()
	log.Printf("swim: failover notified CP for dead node %s", deadAddr)
}

func fetchEpoch(cpAddr string) (uint64, error) {
	resp, err := http.Get(fmt.Sprintf("http://%s/epoch", cpAddr))
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	var out struct {
		Epoch uint64 `json:"epoch"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return 0, err
	}
	return out.Epoch, nil
}

func (gw *gateway) breakerFor(addr string) *breaker.Breaker {
	gw.bmu.RLock()
	b := gw.breakers[addr]
	gw.bmu.RUnlock()
	return b
}

func (gw *gateway) post(ctx context.Context, addr, path string, body []byte) (int, []byte, error) {
	b := gw.breakerFor(addr)
	if b != nil && !b.Allow() {
		return 0, nil, fmt.Errorf("circuit open for %s", addr)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, fmt.Sprintf("http://%s%s", addr, path), bytes.NewReader(body))
	if err != nil {
		return 0, nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Epoch", strconv.FormatUint(gw.epoch.Load(), 10))
	resp, err := gw.client.Do(req)
	if err != nil {
		if b != nil {
			b.Failure()
		}
		return 0, nil, err
	}
	defer resp.Body.Close()
	rb, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 500 {
		if b != nil {
			b.Failure()
		}
	} else {
		if b != nil {
			b.Success()
		}
	}
	if resp.StatusCode == http.StatusConflict {
		var epochResp struct {
			Seen uint64 `json:"seen"`
		}
		if json.Unmarshal(rb, &epochResp) == nil && epochResp.Seen > gw.epoch.Load() {
			gw.epoch.Store(epochResp.Seen)
		}
	}
	return resp.StatusCode, rb, nil
}

func (gw *gateway) handleEvent(w http.ResponseWriter, r *http.Request) {
	select {
	case gw.sem <- struct{}{}:
		defer func() { <-gw.sem }()
	default:
		w.Header().Set("Retry-After", "1")
		http.Error(w, "gateway overloaded", http.StatusServiceUnavailable)
		return
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "read error", http.StatusBadRequest)
		return
	}

	var req struct {
		Key string `json:"key"`
	}
	if err := json.Unmarshal(body, &req); err != nil || req.Key == "" {
		http.Error(w, "id and key required", http.StatusBadRequest)
		return
	}

	if !gw.noLimit && !gw.limiter.Allow(req.Key) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Retry-After", "1")
		w.WriteHeader(http.StatusTooManyRequests)
		fmt.Fprintf(w, `{"error":"rate limited","key":%q}`, req.Key)
		return
	}

	replicas := gw.ring.Replicas(req.Key, gw.rf)
	if !gw.primaryRepl {
		for i, addr := range replicas {
			if !gw.isOverloaded(addr) {
				replicas[0], replicas[i] = replicas[i], replicas[0]
				break
			}
		}
	}

	type result struct {
		status int
		body   []byte
		err    error
	}

	ctx, cancel := context.WithTimeout(r.Context(), 4*time.Second)
	defer cancel()
	send := func() (primaryResult result, acks int) {
		if len(replicas) == 1 || gw.primaryRepl {
			status, rb, err := gw.post(ctx, replicas[0], "/events", body)
			primaryResult = result{status: status, body: rb, err: err}
			if err == nil && (status == http.StatusCreated || status == http.StatusOK || status == http.StatusNoContent || status == http.StatusAccepted) {
				acks++
			}
			return
		}
		ch := make(chan result, len(replicas))
		go func() {
			status, rb, err := gw.post(ctx, replicas[0], "/events", body)
			ch <- result{status: status, body: rb, err: err}
		}()
		for _, replica := range replicas[1:] {
			go func(addr string) {
				rctx, rcancel := context.WithTimeout(context.Background(), 4*time.Second)
				defer rcancel()
				status, _, err := gw.post(rctx, addr, "/replicate", body)
				ch <- result{status: status, err: err}
			}(replica)
		}
		// Wait only until W acks arrive, remaining replicas keep propagating in the background
		for range replicas {
			res := <-ch
			if res.body != nil {
				primaryResult = res
			}
			if res.err == nil && (res.status == http.StatusCreated || res.status == http.StatusOK || res.status == http.StatusNoContent || res.status == http.StatusAccepted) {
				acks++
			}
			if acks >= gw.w && primaryResult.body != nil {
				return
			}
		}
		return
	}

	need := gw.w
	if gw.primaryRepl {
		need = 1
	}

	primaryResult, acks := send()
	// only retry on epoch conflict or total failure
	if acks < need && primaryResult.status != http.StatusTooManyRequests && (primaryResult.status == http.StatusConflict || primaryResult.status == 0) {
		primaryResult, acks = send()
	}

	if acks < need {
		w.Header().Set("Retry-After", "1")
		http.Error(w, fmt.Sprintf("quorum not reached: %d/%d acks", acks, need), http.StatusServiceUnavailable)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if primaryResult.status == 0 {
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}
	w.WriteHeader(primaryResult.status)
	w.Write(primaryResult.body)
}

type aggregateResult struct {
	Counts map[string]uint64 `json:"Counts"`
}

func (gw *gateway) handleAggregates(w http.ResponseWriter, r *http.Request) {
	nodes := gw.ring.Nodes()

	type nodeResult struct {
		data aggregateResult
		err  error
	}

	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()

	results := make([]nodeResult, len(nodes))
	var wg sync.WaitGroup
	for i, n := range nodes {
		wg.Add(1)
		go func(i int, addr string) {
			defer wg.Done()
			req, err := http.NewRequestWithContext(ctx, http.MethodGet, fmt.Sprintf("http://%s/aggregates", addr), nil)
			if err != nil {
				results[i].err = err
				return
			}
			resp, err := gw.client.Do(req)
			if err != nil {
				results[i].err = err
				return
			}
			defer resp.Body.Close()
			json.NewDecoder(resp.Body).Decode(&results[i].data)
		}(i, n)
	}
	wg.Wait()

	merged := make(map[string]uint64)
	for i, res := range results {
		if res.err != nil {
			continue
		}
		addr := nodes[i]
		for k, v := range res.data.Counts {
			if gw.ring.Owner(k) == addr {
				merged[k] += v
			}
		}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{"Counts": merged})
}

func (gw *gateway) handleRing(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("key")
	resp := map[string]any{"nodes": gw.ring.Nodes()}
	if key != "" {
		resp["owner"] = gw.ring.Owner(key)
		resp["replicas"] = gw.ring.Replicas(key, gw.rf)
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

func (gw *gateway) handleHealth(w http.ResponseWriter, r *http.Request) {
	nodes := gw.ring.Nodes()

	type nodeHealth struct {
		Addr         string `json:"addr"`
		Depth        any    `json:"depth"`
		Overloaded   any    `json:"overloaded"`
		BreakerState string `json:"breaker"`
		Err          string `json:"err,omitempty"`
	}

	results := make([]nodeHealth, len(nodes))
	var wg sync.WaitGroup
	for i, n := range nodes {
		wg.Add(1)
		go func(i int, addr string) {
			defer wg.Done()
			results[i].Addr = addr
			if b := gw.breakerFor(addr); b != nil {
				results[i].BreakerState = b.State()
			}
			resp, err := gw.client.Get(fmt.Sprintf("http://%s/health", addr))
			if err != nil {
				results[i].Err = err.Error()
				return
			}
			defer resp.Body.Close()
			var h map[string]any
			if err := json.NewDecoder(resp.Body).Decode(&h); err != nil {
				results[i].Err = err.Error()
				return
			}
			results[i].Depth = h["depth"]
			results[i].Overloaded = h["overloaded"]
		}(i, n)
	}
	wg.Wait()

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(results)
}
