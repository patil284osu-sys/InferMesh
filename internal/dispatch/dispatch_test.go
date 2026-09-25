package dispatch

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestBoundedWaitingAndCompatibleRouting(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(200) }))
	defer backend.Close()
	pool := New([]*Worker{{URL: backend.URL, Model: "demo", Version: "1", Limit: 1}}, 1, false, "round-robin")
	pool.Check(context.Background())
	first, err := pool.Acquire(context.Background(), "demo", "1", false)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { _, err := pool.Acquire(ctx, "demo", "1", false); done <- err }()
	time.Sleep(40 * time.Millisecond)
	_, err = pool.Acquire(context.Background(), "demo", "1", false)
	if !errors.Is(err, ErrFull) {
		t.Fatalf("expected overload, got %v", err)
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("expected cancellation, got %v", err)
	}
	pool.Release(first, false)
	if pool.HasModel("other", "1") {
		t.Fatal("unknown model became eligible")
	}
}

func TestBackgroundCannotTakeInteractiveSlot(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(200) }))
	defer backend.Close()
	pool := New([]*Worker{{URL: backend.URL, Model: "demo", Version: "1", Limit: 2}}, 1, true, "round-robin")
	pool.Check(context.Background())
	background, err := pool.Acquire(context.Background(), "demo", "1", true)
	if err != nil {
		t.Fatal(err)
	}
	interactive, err := pool.Acquire(context.Background(), "demo", "1", false)
	if err != nil {
		t.Fatalf("interactive slot unavailable: %v", err)
	}
	pool.Release(background, true)
	pool.Release(interactive, false)
}

func TestLeastActiveSkipsBusyWorker(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(200) }))
	defer backend.Close()
	first := &Worker{URL: backend.URL, Model: "demo", Version: "1", Limit: 2}
	second := &Worker{URL: backend.URL, Model: "demo", Version: "1", Limit: 2}
	pool := New([]*Worker{first, second}, 1, false, "least-active")
	pool.Check(context.Background())
	busy, err := pool.Acquire(context.Background(), "demo", "1", false)
	if err != nil {
		t.Fatal(err)
	}
	pool.next = 0
	chosen, err := pool.Acquire(context.Background(), "demo", "1", false)
	if err != nil || chosen != second {
		t.Fatalf("wanted idle worker: %v, %v", chosen, err)
	}
	pool.Release(busy, false)
	pool.Release(chosen, false)
}
