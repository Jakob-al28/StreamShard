package membership

import (
	"testing"
	"time"
)

func TestJoinDissemination(t *testing.T) {
	m1, err := New("127.0.0.1:19001", "", nil)
	if err != nil {
		t.Fatal(err)
	}
	m2, err := New("127.0.0.1:19002", "localhost:8082", []string{"127.0.0.1:19001"})
	if err != nil {
		t.Fatal(err)
	}
	_ = m2

	select {
	case ev := <-m1.Events():
		if ev.Type != EventJoin || ev.HTTPAddr != "localhost:8082" {
			t.Fatalf("unexpected event: %+v", ev)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timeout waiting for join event on m1")
	}
}

func TestDeadDetection(t *testing.T) {
	m1, err := New("127.0.0.1:19003", "", nil)
	if err != nil {
		t.Fatal(err)
	}
	m2, err := New("127.0.0.1:19004", "localhost:8084", []string{"127.0.0.1:19003"})
	if err != nil {
		t.Fatal(err)
	}

	// drain the join event
	select {
	case <-m1.Events():
	case <-time.After(3 * time.Second):
		t.Fatal("timeout waiting for join event")
	}

	// close m2's connection to simulate failure
	m2.conn.Close()

	select {
	case ev := <-m1.Events():
		if ev.Type != EventDead || ev.Addr != "127.0.0.1:19004" {
			t.Fatalf("unexpected event: %+v", ev)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("timeout waiting for dead event")
	}
}
