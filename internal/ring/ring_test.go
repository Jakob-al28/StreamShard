package ring

import (
	"fmt"
	"testing"
)

func TestOwnerDeterministic(t *testing.T) {
	nodes := []string{"a:8001", "b:8002", "c:8003"}
	r1 := New(nodes)
	r2 := New([]string{"c:8003", "a:8001", "b:8002"})

	for _, key := range []string{"foo", "bar", "baz", "hello", "world"} {
		if r1.Owner(key) != r2.Owner(key) {
			t.Fatalf("owner mismatch for key %q: %s vs %s", key, r1.Owner(key), r2.Owner(key))
		}
	}
}

func TestOwnerDistribution(t *testing.T) {
	nodes := []string{"a:8001", "b:8002", "c:8003"}
	r := New(nodes)

	counts := make(map[string]int)
	for i := range 3000 {
		key := fmt.Sprintf("key-%d", i)
		counts[r.Owner(key)]++
	}

	for _, n := range nodes {
		frac := float64(counts[n]) / 3000.0
		if frac < 0.2 || frac > 0.5 {
			t.Errorf("node %s got %.1f%% of keys, want 20-50%%", n, frac*100)
		}
	}
}

func TestReplicasCount(t *testing.T) {
	r := New([]string{"a:8001", "b:8002", "c:8003", "d:8004"})

	for _, key := range []string{"foo", "bar", "baz"} {
		replicas := r.Replicas(key, 3)
		if len(replicas) != 3 {
			t.Fatalf("expected 3 replicas for %q, got %d", key, len(replicas))
		}
		seen := make(map[string]struct{})
		for _, n := range replicas {
			if _, dup := seen[n]; dup {
				t.Fatalf("duplicate replica %s for key %q", n, key)
			}
			seen[n] = struct{}{}
		}
	}
}

func TestReplicasCapAtNodeCount(t *testing.T) {
	r := New([]string{"a:8001", "b:8002"})
	replicas := r.Replicas("key", 5)
	if len(replicas) != 2 {
		t.Fatalf("expected replicas capped at 2, got %d", len(replicas))
	}
}

func TestSingleNode(t *testing.T) {
	r := New([]string{"solo:8001"})
	if r.Owner("anything") != "solo:8001" {
		t.Fatal("single node should own everything")
	}
}
