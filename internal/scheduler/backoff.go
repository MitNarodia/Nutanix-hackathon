package scheduler

import (
	"math/rand"
	"sync"
	"time"
)

type PeerHealth struct {
	IsHot         bool
	CooldownUntil time.Time
	FailCount     int
}

type HealthTracker struct {
	mu    sync.Mutex
	peers map[string]*PeerHealth
}

func NewHealthTracker() *HealthTracker {
	return &HealthTracker{
		peers: make(map[string]*PeerHealth),
	}
}

func (h *HealthTracker) MarkHot(peerAddr string, retryAfter time.Duration) {
	h.mu.Lock()
	defer h.mu.Unlock()

	p, exists := h.peers[peerAddr]
	if !exists {
		p = &PeerHealth{}
		h.peers[peerAddr] = p
	}

	p.IsHot = true
	p.FailCount++
	p.CooldownUntil = time.Now().Add(retryAfter)
}

func (h *HealthTracker) IsAvailable(peerAddr string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()

	p, exists := h.peers[peerAddr]
	if !exists {
		return true
	}

	if p.IsHot {
		if time.Now().After(p.CooldownUntil) {
			p.IsHot = false
			p.FailCount = 0
			return true
		}

		return false
	}

	return true
}

func CalcJitteredBackoff(attempt int, base time.Duration, max time.Duration) time.Duration {
	if attempt > 10 {
		attempt = 10
	}

	multiplier := 1 << uint(attempt)
	backoff := base * time.Duration(multiplier)

	if backoff > max {
		backoff = max
	}

	jitterRange := float64(backoff) * 0.25
	jitter := (rand.Float64() * 2 * jitterRange) - jitterRange

	finalDuration := time.Duration(float64(backoff) + jitter)

	if finalDuration > max {
		return max
	}

	if finalDuration < time.Millisecond {
		return time.Millisecond
	}

	return finalDuration
}