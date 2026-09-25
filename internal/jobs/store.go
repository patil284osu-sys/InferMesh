package jobs

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"time"

	_ "modernc.org/sqlite"
)

var ErrConflict = errors.New("idempotency key used for another request")
var ErrMissing = errors.New("job not found")

type Job struct {
	ID      string          `json:"id"`
	Status  string          `json:"status"`
	Model   string          `json:"model"`
	Version string          `json:"version"`
	Image   string          `json:"-"`
	Result  json.RawMessage `json:"result,omitempty"`
	Error   string          `json:"error,omitempty"`
	Attempt int             `json:"attempt"`
}

type Store struct{ DB *sql.DB }

func Open(path string) (*Store, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	for _, statement := range []string{
		"PRAGMA journal_mode=WAL",
		"PRAGMA busy_timeout=5000",
		`CREATE TABLE IF NOT EXISTS jobs (
			id TEXT PRIMARY KEY, model TEXT NOT NULL, version TEXT NOT NULL,
			image TEXT NOT NULL, fingerprint TEXT NOT NULL, idem_key TEXT UNIQUE,
			status TEXT NOT NULL, result BLOB, error TEXT NOT NULL DEFAULT '',
			attempt INTEGER NOT NULL DEFAULT 0, lease_until INTEGER NOT NULL DEFAULT 0)`,
		`CREATE TABLE IF NOT EXISTS outbox (job_id TEXT PRIMARY KEY, published INTEGER NOT NULL DEFAULT 0)`,
	} {
		if _, err := db.Exec(statement); err != nil {
			db.Close()
			return nil, err
		}
	}
	return &Store{DB: db}, nil
}

func (s *Store) Submit(ctx context.Context, model, version, image, key string) (string, error) {
	fingerprint := sha256.Sum256([]byte(model + "\x00" + version + "\x00" + image))
	idBytes := make([]byte, 16)
	if _, err := rand.Read(idBytes); err != nil {
		return "", err
	}
	id := hex.EncodeToString(idBytes)
	var idempotency any
	if key != "" {
		idempotency = key
	}
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return "", err
	}
	defer tx.Rollback()
	if key != "" {
		var existingID, existingFingerprint string
		err = tx.QueryRowContext(ctx, "SELECT id, fingerprint FROM jobs WHERE idem_key=?", key).Scan(&existingID, &existingFingerprint)
		if err == nil {
			if existingFingerprint != hex.EncodeToString(fingerprint[:]) {
				return "", ErrConflict
			}
			return existingID, tx.Commit()
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return "", err
		}
	}
	_, err = tx.ExecContext(ctx, "INSERT INTO jobs(id, model, version, image, fingerprint, idem_key, status) VALUES(?,?,?,?,?,?,'queued')", id, model, version, image, hex.EncodeToString(fingerprint[:]), idempotency)
	if err != nil {
		return "", err
	}
	if _, err = tx.ExecContext(ctx, "INSERT INTO outbox(job_id) VALUES(?)", id); err != nil {
		return "", err
	}
	return id, tx.Commit()
}

func (s *Store) Get(ctx context.Context, id string) (Job, error) {
	var job Job
	var result []byte
	err := s.DB.QueryRowContext(ctx, "SELECT id, status, model, version, image, result, error, attempt FROM jobs WHERE id=?", id).
		Scan(&job.ID, &job.Status, &job.Model, &job.Version, &job.Image, &result, &job.Error, &job.Attempt)
	if errors.Is(err, sql.ErrNoRows) {
		return job, ErrMissing
	}
	job.Result = result
	return job, err
}

func (s *Store) Cancel(ctx context.Context, id string) (Job, error) {
	_, err := s.DB.ExecContext(ctx, "UPDATE jobs SET status='cancelled' WHERE id=? AND status IN ('queued','running')", id)
	if err != nil {
		return Job{}, err
	}
	return s.Get(ctx, id)
}

func (s *Store) Claim(ctx context.Context, id string) (Job, bool, error) {
	now := time.Now().UnixMilli()
	result, err := s.DB.ExecContext(ctx, `UPDATE jobs SET status='running', attempt=attempt+1, lease_until=?
		WHERE id=? AND (status='queued' OR (status='running' AND lease_until<?))`, now+45000, id, now)
	if err != nil {
		return Job{}, false, err
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return Job{}, false, err
	}
	job, err := s.Get(ctx, id)
	return job, changed == 1, err
}

func (s *Store) Finish(ctx context.Context, id string, attempt int, status string, value json.RawMessage, message string) (bool, error) {
	result, err := s.DB.ExecContext(ctx, `UPDATE jobs SET status=?, result=?, error=?, lease_until=0
		WHERE id=? AND status='running' AND attempt=?`, status, value, message, id, attempt)
	if err != nil {
		return false, err
	}
	changed, err := result.RowsAffected()
	return changed == 1, err
}

func (s *Store) Unpublished(ctx context.Context) ([]string, error) {
	rows, err := s.DB.QueryContext(ctx, "SELECT job_id FROM outbox WHERE published=0 LIMIT 32")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

func (s *Store) Published(ctx context.Context, id string) error {
	_, err := s.DB.ExecContext(ctx, "UPDATE outbox SET published=1 WHERE job_id=?", id)
	return err
}
