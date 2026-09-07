package daemon

import (
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// isSubPath reports whether candidate is inside (or equal to) base, after
// resolving both to absolute paths.
func isSubPath(base, candidate string) bool {
	absBase, err := filepath.Abs(base)
	if err != nil {
		return false
	}

	absCandidate, err := filepath.Abs(candidate)
	if err != nil {
		return false
	}

	rel, err := filepath.Rel(absBase, absCandidate)
	if err != nil {
		return false
	}

	if rel == "." {
		return true
	}

	return rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator))
}

// quietSet tracks paths the sync engine is currently writing to
type quietSet struct {
	mu  sync.Mutex
	ttl time.Duration
	m   map[string]time.Time
}

func newQuietSet(ttl time.Duration) *quietSet {
	return &quietSet{
		ttl: ttl,
		m:   make(map[string]time.Time),
	}
}

func (q *quietSet) Mark(path string) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.m[path] = time.Now()
}

func (q *quietSet) IsQuiet(path string) bool {
	q.mu.Lock()
	defer q.mu.Unlock()

	t, ok := q.m[path]
	if !ok {
		return false
	}

	if time.Since(t) > q.ttl {
		delete(q.m, path)
		return false
	}

	return true
}
