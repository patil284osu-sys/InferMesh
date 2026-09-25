package dispatch

import (
	"context"
	"errors"
	"net/http"
	"sync"
	"time"
)

var ErrFull = errors.New("queue full")
var ErrNoWorker = errors.New("no compatible worker")

type Worker struct {
	URL         string
	Model       string
	Version     string
	Limit       int
	interactive int
	background  int
	ready       bool
}

type Pool struct {
	mu                sync.Mutex
	workers           []*Worker
	next              int
	waiting           chan struct{}
	client            *http.Client
	reserveBackground bool
	policy            string
}

func New(workers []*Worker, queueSize int, reserveBackground bool, policy string) *Pool {
	return &Pool{workers: workers, waiting: make(chan struct{}, queueSize), client: &http.Client{Timeout: 2 * time.Second}, reserveBackground: reserveBackground, policy: policy}
}

func (p *Pool) Check(ctx context.Context) {
	p.mu.Lock()
	workers := append([]*Worker(nil), p.workers...)
	p.mu.Unlock()
	for _, worker := range workers {
		checkCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
		request, _ := http.NewRequestWithContext(checkCtx, http.MethodGet, worker.URL+"/ready", nil)
		response, err := p.client.Do(request)
		ready := err == nil && response.StatusCode == http.StatusOK
		if err == nil {
			response.Body.Close()
		}
		cancel()
		p.mu.Lock()
		worker.ready = ready
		p.mu.Unlock()
	}
}

func (p *Pool) HasModel(model, version string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, worker := range p.workers {
		if worker.Model == model && worker.Version == version {
			return true
		}
	}
	return false
}

func (p *Pool) Acquire(ctx context.Context, model, version string, background bool) (*Worker, error) {
	if !background {
		select {
		case p.waiting <- struct{}{}:
		default:
			return nil, ErrFull
		}
		defer func() { <-p.waiting }()
	}
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		p.mu.Lock()
		chosen := -1
		lowest := int(^uint(0) >> 1)
		for offset := range p.workers {
			index := (p.next + offset) % len(p.workers)
			worker := p.workers[index]
			interactiveLimit := worker.Limit
			if p.reserveBackground {
				interactiveLimit--
			}
			available := worker.interactive < interactiveLimit
			if background {
				available = p.reserveBackground && worker.background < 1
			}
			if worker.Model == model && worker.Version == version && worker.ready && available {
				active := worker.interactive + worker.background
				if chosen < 0 || (p.policy == "least-active" && active < lowest) {
					chosen, lowest = index, active
				}
				if p.policy == "round-robin" {
					break
				}
			}
		}
		if chosen >= 0 {
			worker := p.workers[chosen]
			if background {
				worker.background++
			} else {
				worker.interactive++
			}
			p.next = (chosen + 1) % len(p.workers)
			p.mu.Unlock()
			return worker, nil
		}
		p.mu.Unlock()
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-ticker.C:
		}
	}
}

func (p *Pool) Release(worker *Worker, background bool) {
	p.mu.Lock()
	if background {
		worker.background--
	} else {
		worker.interactive--
	}
	p.mu.Unlock()
}

func (p *Pool) MarkFailed(worker *Worker) {
	p.mu.Lock()
	worker.ready = false
	p.mu.Unlock()
}

func (p *Pool) Snapshot() (waiting, activeInteractive, activeBackground int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, worker := range p.workers {
		activeInteractive += worker.interactive
		activeBackground += worker.background
	}
	return len(p.waiting), activeInteractive, activeBackground
}
