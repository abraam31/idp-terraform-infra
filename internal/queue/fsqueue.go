package queue

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"time"
)

type FSQueue struct {
	BaseDir string
}

type JobEnvelope struct {
	ID        string          `json:"id"`
	Kind      string          `json:"kind"`
	CreatedAt time.Time       `json:"created_at"`
	Payload   json.RawMessage `json:"payload"`
}

func NewFSQueue(baseDir string) (*FSQueue, error) {
	if baseDir == "" {
		return nil, errors.New("baseDir is required")
	}
	err := os.MkdirAll(filepath.Join(baseDir, "pending"), 0o755)
	if err != nil {
		return nil, err
	}
	err = os.MkdirAll(filepath.Join(baseDir, "processing"), 0o755)
	if err != nil {
		return nil, err
	}
	err = os.MkdirAll(filepath.Join(baseDir, "done"), 0o755)
	if err != nil {
		return nil, err
	}
	return &FSQueue{BaseDir: baseDir}, nil
}

func (q *FSQueue) Enqueue(kind string, payload []byte) (string, error) {
	id := newID()
	env := JobEnvelope{
		ID:        id,
		Kind:      kind,
		CreatedAt: time.Now().UTC(),
		Payload:   append([]byte(nil), payload...),
	}
	b, err := json.MarshalIndent(env, "", "  ")
	if err != nil {
		return "", err
	}
	path := filepath.Join(q.BaseDir, "pending", id+".json")
	if err := os.WriteFile(path, b, 0o644); err != nil {
		return "", err
	}
	return id, nil
}

// Dequeue moves one job file from pending/ to processing/ and returns its envelope and path.
func (q *FSQueue) Dequeue() (*JobEnvelope, string, error) {
	pendingDir := filepath.Join(q.BaseDir, "pending")
	entries, err := os.ReadDir(pendingDir)
	if err != nil {
		return nil, "", err
	}
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		src := filepath.Join(pendingDir, name)
		dst := filepath.Join(q.BaseDir, "processing", name)
		// best-effort exclusive claim
		if err := os.Rename(src, dst); err != nil {
			continue
		}
		data, err := os.ReadFile(dst)
		if err != nil {
			return nil, "", err
		}
		var env JobEnvelope
		if err := json.Unmarshal(data, &env); err != nil {
			return nil, "", err
		}
		return &env, dst, nil
	}
	return nil, "", fs.ErrNotExist
}

// MarkDone moves a processing file to done/.
func (q *FSQueue) MarkDone(processingPath string) error {
	_, name := filepath.Split(processingPath)
	dst := filepath.Join(q.BaseDir, "done", name)
	return os.Rename(processingPath, dst)
}

func newID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

