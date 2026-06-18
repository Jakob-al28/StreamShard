package ring

import (
	"fmt"
	"hash/fnv"
	"sort"
	"sync"
)

const vnodes = 150

type point struct {
	hash uint32
	node string
}

type Ring struct {
	mu     sync.RWMutex
	points []point
	nodes  []string
}

func New(nodes []string) *Ring {
	r := &Ring{}
	seen := make(map[string]struct{})
	for _, n := range nodes {
		if _, ok := seen[n]; ok {
			continue
		}
		seen[n] = struct{}{}
		r.nodes = append(r.nodes, n)
	}
	sort.Strings(r.nodes)

	for _, n := range r.nodes {
		for i := range vnodes {
			r.points = append(r.points, point{
				hash: hashKey(fmt.Sprintf("%s#%d", n, i)),
				node: n,
			})
		}
	}
	sort.Slice(r.points, func(i, j int) bool {
		return r.points[i].hash < r.points[j].hash
	})
	return r
}

func (r *Ring) Add(node string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, n := range r.nodes {
		if n == node {
			return
		}
	}
	r.nodes = append(r.nodes, node)
	sort.Strings(r.nodes)
	for i := range vnodes {
		r.points = append(r.points, point{
			hash: hashKey(fmt.Sprintf("%s#%d", node, i)),
			node: node,
		})
	}
	sort.Slice(r.points, func(i, j int) bool {
		return r.points[i].hash < r.points[j].hash
	})
}

func (r *Ring) Remove(node string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	nodes := r.nodes[:0]
	for _, n := range r.nodes {
		if n != node {
			nodes = append(nodes, n)
		}
	}
	r.nodes = nodes
	points := r.points[:0]
	for _, p := range r.points {
		if p.node != node {
			points = append(points, p)
		}
	}
	r.points = points
}

func (r *Ring) Owner(key string) string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if len(r.points) == 0 {
		return ""
	}
	h := hashKey(key)
	i := sort.Search(len(r.points), func(i int) bool {
		return r.points[i].hash >= h
	})
	return r.points[i%len(r.points)].node
}

func (r *Ring) Replicas(key string, rf int) []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if len(r.nodes) == 0 {
		return nil
	}
	if rf > len(r.nodes) {
		rf = len(r.nodes)
	}
	h := hashKey(key)
	i := sort.Search(len(r.points), func(i int) bool {
		return r.points[i].hash >= h
	})

	seen := make(map[string]struct{})
	var result []string
	for len(result) < rf {
		n := r.points[i%len(r.points)].node
		if _, ok := seen[n]; !ok {
			seen[n] = struct{}{}
			result = append(result, n)
		}
		i++
	}
	return result
}

func (r *Ring) Nodes() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]string, len(r.nodes))
	copy(out, r.nodes)
	return out
}

func hashKey(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}
