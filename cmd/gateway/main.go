package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"

	"github.com/patil284osu-sys/InferMesh/internal/dispatch"
	"github.com/patil284osu-sys/InferMesh/internal/jobs"
)

type request struct {
	Model      string `json:"model"`
	Version    string `json:"version"`
	Prompt     string `json:"prompt,omitempty"`
	Image      string `json:"image,omitempty"`
	MaxTokens  int    `json:"max_tokens,omitempty"`
	DeadlineMS int    `json:"deadline_ms,omitempty"`
}

type server struct {
	pool           *dispatch.Pool
	client         *http.Client
	store          *jobs.Store
	runner         *jobs.Runner
	requests       atomic.Int64
	overload       atomic.Int64
	workerErrors   atomic.Int64
	queueWaitNS    atomic.Int64
	queueWaitCount atomic.Int64
}

func main() {
	port := flag.String("port", "8080", "listen port")
	addresses := flag.String("workers", "demo@1@http://localhost:9001,demo@1@http://localhost:9002", "comma-separated model@version@URL entries")
	queue := flag.Int("queue", 16, "maximum waiting requests")
	capacity := flag.Int("capacity", 2, "concurrent calls per worker")
	database := flag.String("db", "", "SQLite path; enables durable jobs")
	natsURL := flag.String("nats", "nats://localhost:4222", "JetStream URL")
	policy := flag.String("policy", "round-robin", "round-robin or least-active")
	flag.Parse()
	if *policy != "round-robin" && *policy != "least-active" {
		log.Fatal("invalid policy")
	}
	if *queue < 1 || *capacity < 1 {
		log.Fatal("queue and capacity must be positive")
	}
	if *database != "" && *capacity < 2 {
		log.Fatal("durable jobs require capacity of at least 2 per worker")
	}
	var workers []*dispatch.Worker
	for _, entry := range strings.Split(*addresses, ",") {
		parts := strings.SplitN(strings.TrimSpace(entry), "@", 3)
		if len(parts) != 3 || parts[0] == "" || parts[1] == "" {
			log.Fatal("invalid worker entry: ", entry)
		}
		address := strings.TrimRight(parts[2], "/")
		if !strings.HasPrefix(address, "http://") && !strings.HasPrefix(address, "https://") {
			log.Fatal("invalid worker URL: ", address)
		}
		workers = append(workers, &dispatch.Worker{URL: address, Model: parts[0], Version: parts[1], Limit: *capacity})
	}
	pool := dispatch.New(workers, *queue, *database != "", *policy)
	s := &server{pool: pool, client: &http.Client{Transport: &http.Transport{MaxIdleConnsPerHost: 16}}}
	pool.Check(context.Background())
	go func() {
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for range ticker.C {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			pool.Check(ctx)
			cancel()
		}
	}()
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) { w.Write([]byte("ok\n")) })
	mux.HandleFunc("GET /metrics", s.metrics)
	mux.HandleFunc("POST /generate", s.handleGenerate)
	mux.HandleFunc("POST /predict", s.handlePredict)
	if *database != "" {
		store, err := jobs.Open(filepath.Clean(*database))
		if err != nil {
			log.Fatal(err)
		}
		s.store = store
		runner, err := jobs.NewRunner(store, *natsURL, s.runJob)
		if err != nil {
			log.Fatal(err)
		}
		s.runner = runner
		runner.Start(context.Background())
		mux.HandleFunc("POST /jobs", s.submitJob)
		mux.HandleFunc("GET /jobs/{id}", s.getJob)
		mux.HandleFunc("POST /jobs/{id}/cancel", s.cancelJob)
	}
	log.Printf("InferMesh gateway listening on 127.0.0.1:%s", *port)
	log.Fatal(http.ListenAndServe("127.0.0.1:"+*port, mux))
}

func (s *server) parse(w http.ResponseWriter, r *http.Request, kind string) (request, context.Context, context.CancelFunc, bool) {
	var input request
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&input) != nil || decoder.Decode(new(any)) != io.EOF {
		http.Error(w, "invalid JSON or oversized payload", 400)
		return input, nil, nil, false
	}
	if !s.pool.HasModel(input.Model, input.Version) {
		http.Error(w, "unknown model/version", 400)
		return input, nil, nil, false
	}
	if kind == "job" && input.DeadlineMS != 0 {
		http.Error(w, "job deadline_ms is not supported", 400)
		return input, nil, nil, false
	}
	if input.DeadlineMS == 0 {
		input.DeadlineMS = 5000
	}
	if input.DeadlineMS < 1 || input.DeadlineMS > 30000 {
		http.Error(w, "invalid deadline_ms", 400)
		return input, nil, nil, false
	}
	if kind == "generate" && (input.Prompt == "" || input.Image != "" || input.MaxTokens < 1 || input.MaxTokens > 512) {
		http.Error(w, "invalid generation request", 400)
		return input, nil, nil, false
	}
	if (kind == "predict" || kind == "job") && (input.Image == "" || input.Prompt != "" || input.MaxTokens != 0) {
		http.Error(w, "invalid prediction request", 400)
		return input, nil, nil, false
	}
	ctx, cancel := context.WithTimeout(r.Context(), time.Duration(input.DeadlineMS)*time.Millisecond)
	return input, ctx, cancel, true
}

func (s *server) call(ctx context.Context, worker *dispatch.Worker, path string, input request) (*http.Response, error) {
	body, _ := json.Marshal(input)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, worker.URL+path, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	return s.client.Do(req)
}

func (s *server) acquire(w http.ResponseWriter, ctx context.Context, input request) (*dispatch.Worker, bool) {
	start := time.Now()
	worker, err := s.pool.Acquire(ctx, input.Model, input.Version, false)
	if errors.Is(err, dispatch.ErrFull) {
		s.overload.Add(1)
		http.Error(w, "queue full", http.StatusTooManyRequests)
		return nil, false
	}
	if err != nil {
		http.Error(w, "deadline or cancellation", http.StatusGatewayTimeout)
		return nil, false
	}
	s.queueWaitNS.Add(time.Since(start).Nanoseconds())
	s.queueWaitCount.Add(1)
	return worker, true
}

func (s *server) handlePredict(w http.ResponseWriter, r *http.Request) {
	s.requests.Add(1)
	input, ctx, cancel, ok := s.parse(w, r, "predict")
	if !ok {
		return
	}
	defer cancel()
	worker, ok := s.acquire(w, ctx, input)
	if !ok {
		return
	}
	defer s.pool.Release(worker, false)
	response, err := s.call(ctx, worker, "/predict", input)
	if err != nil {
		s.workerErrors.Add(1)
		if ctx.Err() == nil {
			s.pool.MarkFailed(worker)
		}
		http.Error(w, "worker unavailable", 502)
		return
	}
	defer response.Body.Close()
	if response.StatusCode != 200 {
		http.Error(w, "worker failed", 502)
		return
	}
	result, err := io.ReadAll(io.LimitReader(response.Body, 1<<20))
	if err != nil || !json.Valid(result) {
		http.Error(w, "invalid worker response", 502)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Write(result)
}

func (s *server) handleGenerate(w http.ResponseWriter, r *http.Request) {
	s.requests.Add(1)
	input, ctx, cancel, ok := s.parse(w, r, "generate")
	if !ok {
		return
	}
	defer cancel()
	worker, ok := s.acquire(w, ctx, input)
	if !ok {
		return
	}
	defer s.pool.Release(worker, false)
	response, err := s.call(ctx, worker, "/generate", input)
	if err != nil {
		s.workerErrors.Add(1)
		if ctx.Err() == nil {
			s.pool.MarkFailed(worker)
		}
		http.Error(w, "worker unavailable", 502)
		return
	}
	defer response.Body.Close()
	if response.StatusCode != 200 {
		http.Error(w, "worker failed", 502)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	decoder := json.NewDecoder(io.LimitReader(response.Body, 1<<20))
	for {
		var event struct {
			Token  string `json:"token"`
			Worker string `json:"worker"`
			Done   bool   `json:"done"`
		}
		err := decoder.Decode(&event)
		if err == nil && event.Done && ctx.Err() == nil {
			fmt.Fprint(w, "event: completed\ndata: {}\n\n")
			w.(http.Flusher).Flush()
			return
		}
		if err != nil || event.Token == "" {
			fmt.Fprint(w, "event: failed\ndata: {\"error\":\"stream interrupted\"}\n\n")
			w.(http.Flusher).Flush()
			return
		}
		payload, _ := json.Marshal(event)
		fmt.Fprintf(w, "event: token\ndata: %s\n\n", payload)
		w.(http.Flusher).Flush()
	}
}

func (s *server) submitJob(w http.ResponseWriter, r *http.Request) {
	input, _, cancel, ok := s.parse(w, r, "job")
	if !ok {
		return
	}
	cancel()
	key := r.Header.Get("Idempotency-Key")
	if len(key) > 128 {
		http.Error(w, "idempotency key too long", 400)
		return
	}
	id, err := s.store.Submit(r.Context(), input.Model, input.Version, input.Image, key)
	if errors.Is(err, jobs.ErrConflict) {
		http.Error(w, "idempotency key conflict", 409)
		return
	}
	if err != nil {
		http.Error(w, "job store unavailable", 503)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	json.NewEncoder(w).Encode(map[string]string{"id": id, "status": "queued"})
}

func (s *server) getJob(w http.ResponseWriter, r *http.Request) {
	job, err := s.store.Get(r.Context(), r.PathValue("id"))
	if errors.Is(err, jobs.ErrMissing) {
		http.Error(w, "unknown job", 404)
		return
	}
	if err != nil {
		http.Error(w, "job store unavailable", 503)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(job)
}

func (s *server) cancelJob(w http.ResponseWriter, r *http.Request) {
	job, err := s.store.Cancel(r.Context(), r.PathValue("id"))
	if errors.Is(err, jobs.ErrMissing) {
		http.Error(w, "unknown job", 404)
		return
	}
	if err != nil {
		http.Error(w, "job store unavailable", 503)
		return
	}
	s.runner.CancelActive(job.ID)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(job)
}

func (s *server) runJob(ctx context.Context, job jobs.Job) (json.RawMessage, error) {
	worker, err := s.pool.Acquire(ctx, job.Model, job.Version, true)
	if err != nil {
		return nil, err
	}
	defer s.pool.Release(worker, true)
	response, err := s.call(ctx, worker, "/predict", request{Model: job.Model, Version: job.Version, Image: job.Image})
	if err != nil {
		if ctx.Err() == nil {
			s.pool.MarkFailed(worker)
		}
		return nil, err
	}
	defer response.Body.Close()
	if response.StatusCode != 200 {
		return nil, fmt.Errorf("worker status %d", response.StatusCode)
	}
	result, err := io.ReadAll(io.LimitReader(response.Body, 1<<20))
	if err != nil || !json.Valid(result) {
		return nil, errors.New("invalid worker result")
	}
	return result, nil
}

func (s *server) metrics(w http.ResponseWriter, r *http.Request) {
	waiting, interactive, background := s.pool.Snapshot()
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	fmt.Fprintf(w, "infermesh_interactive_requests_total %d\n", s.requests.Load())
	fmt.Fprintf(w, "infermesh_overload_total %d\n", s.overload.Load())
	fmt.Fprintf(w, "infermesh_worker_errors_total %d\n", s.workerErrors.Load())
	fmt.Fprintf(w, "infermesh_queue_wait_seconds_sum %f\n", float64(s.queueWaitNS.Load())/float64(time.Second))
	fmt.Fprintf(w, "infermesh_queue_wait_seconds_count %d\n", s.queueWaitCount.Load())
	fmt.Fprintf(w, "infermesh_waiting %d\n", waiting)
	fmt.Fprintf(w, "infermesh_active_interactive %d\n", interactive)
	fmt.Fprintf(w, "infermesh_active_background %d\n", background)
}
