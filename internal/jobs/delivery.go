package jobs

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
)

type Runner struct {
	Store   *Store
	js      nats.JetStreamContext
	sub     *nats.Subscription
	Run     func(context.Context, Job) (json.RawMessage, error)
	mu      sync.Mutex
	cancels map[string]context.CancelFunc
}

func NewRunner(store *Store, url string, run func(context.Context, Job) (json.RawMessage, error)) (*Runner, error) {
	conn, err := nats.Connect(url, nats.RetryOnFailedConnect(true), nats.MaxReconnects(-1))
	if err != nil {
		return nil, err
	}
	js, err := conn.JetStream()
	if err != nil {
		conn.Close()
		return nil, err
	}
	_, err = js.StreamInfo("INFERMESH")
	if errors.Is(err, nats.ErrStreamNotFound) {
		_, err = js.AddStream(&nats.StreamConfig{Name: "INFERMESH", Subjects: []string{"infermesh.jobs"}, Storage: nats.FileStorage})
	}
	if err != nil {
		conn.Close()
		return nil, err
	}
	sub, err := js.PullSubscribe("infermesh.jobs", "gateway", nats.BindStream("INFERMESH"), nats.AckWait(60*time.Second))
	if err != nil {
		conn.Close()
		return nil, err
	}
	return &Runner{Store: store, js: js, sub: sub, Run: run, cancels: make(map[string]context.CancelFunc)}, nil
}

func (r *Runner) CancelActive(id string) {
	r.mu.Lock()
	cancel := r.cancels[id]
	r.mu.Unlock()
	if cancel != nil {
		cancel()
	}
}

func (r *Runner) Start(ctx context.Context) {
	go r.publishLoop(ctx)
	go r.consumeLoop(ctx)
}

func (r *Runner) publishLoop(ctx context.Context) {
	ticker := time.NewTicker(300 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		ids, err := r.Store.Unpublished(ctx)
		if err != nil {
			log.Printf("outbox read: %v", err)
			continue
		}
		for _, id := range ids {
			if _, err := r.js.Publish("infermesh.jobs", []byte(id), nats.MsgId(id)); err != nil {
				log.Printf("outbox publish: %v", err)
				break
			}
			if err := r.Store.Published(ctx, id); err != nil {
				log.Printf("outbox mark: %v", err)
				break
			}
		}
	}
}

func (r *Runner) consumeLoop(ctx context.Context) {
	for ctx.Err() == nil {
		messages, err := r.sub.Fetch(1, nats.MaxWait(time.Second))
		if err != nil {
			if !errors.Is(err, nats.ErrTimeout) {
				log.Printf("job fetch: %v", err)
			}
			continue
		}
		for _, message := range messages {
			r.process(ctx, message)
		}
	}
}

func (r *Runner) process(ctx context.Context, message *nats.Msg) {
	id := string(message.Data)
	job, claimed, err := r.Store.Claim(ctx, id)
	if errors.Is(err, ErrMissing) {
		message.Ack()
		return
	}
	if err != nil {
		log.Printf("job claim: %v", err)
		message.NakWithDelay(time.Second)
		return
	}
	if !claimed {
		if job.Status == "running" {
			message.NakWithDelay(5 * time.Second)
		} else {
			message.Ack()
		}
		return
	}
	workCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	r.mu.Lock()
	r.cancels[id] = cancel
	r.mu.Unlock()
	current, checkErr := r.Store.Get(ctx, id)
	if checkErr != nil || current.Status != "running" || current.Attempt != job.Attempt {
		cancel()
	}
	result, workErr := r.Run(workCtx, job)
	cancel()
	r.mu.Lock()
	delete(r.cancels, id)
	r.mu.Unlock()
	status, detail := "succeeded", ""
	if workErr != nil {
		status, detail = "queued", workErr.Error()
		if job.Attempt >= 3 {
			status = "failed"
		}
	}
	updated, err := r.Store.Finish(ctx, id, job.Attempt, status, result, detail)
	if err != nil {
		log.Printf("job finish: %v", err)
		message.NakWithDelay(time.Second)
		return
	}
	if !updated || status != "queued" {
		message.Ack()
		return
	}
	message.NakWithDelay(time.Duration(job.Attempt) * time.Second)
}
