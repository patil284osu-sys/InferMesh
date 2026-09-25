package jobs

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"testing"
)

func TestAcceptedJobSurvivesRestartAndOldAttemptCannotOverwrite(t *testing.T) {
	path := filepath.Join(t.TempDir(), "jobs.db")
	store, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	id, err := store.Submit(ctx, "demo", "1", "image", "intent-1")
	if err != nil {
		t.Fatal(err)
	}
	again, err := store.Submit(ctx, "demo", "1", "image", "intent-1")
	if err != nil || again != id {
		t.Fatalf("duplicate ID: %q, %v", again, err)
	}
	if _, err := store.Submit(ctx, "demo", "1", "different", "intent-1"); !errors.Is(err, ErrConflict) {
		t.Fatalf("expected conflict: %v", err)
	}
	store.DB.Close()
	store, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer store.DB.Close()
	job, err := store.Get(ctx, id)
	if err != nil || job.Status != "queued" {
		t.Fatalf("recovery: %+v %v", job, err)
	}
	ids, err := store.Unpublished(ctx)
	if err != nil || len(ids) != 1 || ids[0] != id {
		t.Fatalf("outbox: %v %v", ids, err)
	}
	job, claimed, err := store.Claim(ctx, id)
	if err != nil || !claimed || job.Attempt != 1 {
		t.Fatalf("claim: %+v %v %v", job, claimed, err)
	}
	if _, err := store.Finish(ctx, id, 1, "queued", nil, "retry"); err != nil {
		t.Fatal(err)
	}
	job, claimed, err = store.Claim(ctx, id)
	if err != nil || !claimed || job.Attempt != 2 {
		t.Fatalf("reclaim: %+v %v %v", job, claimed, err)
	}
	updated, err := store.Finish(ctx, id, 1, "succeeded", json.RawMessage(`{"wrong":true}`), "")
	if err != nil || updated {
		t.Fatalf("stale result accepted: %v %v", updated, err)
	}
	updated, err = store.Finish(ctx, id, 2, "succeeded", json.RawMessage(`{"label":"demo"}`), "")
	if err != nil || !updated {
		t.Fatalf("current result rejected: %v %v", updated, err)
	}
}

func TestCancelledJobRejectsLateResult(t *testing.T) {
	store, err := Open(filepath.Join(t.TempDir(), "jobs.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.DB.Close()
	ctx := context.Background()
	id, err := store.Submit(ctx, "demo", "1", "image", "")
	if err != nil {
		t.Fatal(err)
	}
	job, _, err := store.Claim(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Cancel(ctx, id); err != nil {
		t.Fatal(err)
	}
	updated, err := store.Finish(ctx, id, job.Attempt, "succeeded", json.RawMessage(`{}`), "")
	if err != nil || updated {
		t.Fatalf("cancelled result accepted: %v %v", updated, err)
	}
}
