package scheduler

import (
	"testing"
	"time"
)

func TestHealthTrackerAndBackoff(t *testing.T) {
	tracker := NewHealthTracker()
	peer := "127.0.0.1:9200"

	// test - 1 -> Initial state should be available
	if !tracker.IsAvailable(peer) {
		t.Fatalf("Expected peer %s to be available initially", peer)
	}

	// test - 2 -> Mark peer as hot due to a 429 response
	tracker.MarkHot(peer, 1*time.Second)

	// test - 3 -> Peer should now be unavailable (cooling down)
	if tracker.IsAvailable(peer) {
		t.Fatalf("Expected peer %s to be unavailable during cooldown", peer)
	}

	// test -4 -> Wait for cooldown to expire
	time.Sleep(1100 * time.Millisecond)

	// tets - 5 -> Peer should automatically recover and be available again
	if !tracker.IsAvailable(peer) {
		t.Fatalf("Expected peer %s to recover after cooldown expiration", peer)
	}

	// test - 6 -> Test Jittered Backoff bounds
	base := 1 * time.Second
	max := 30 * time.Second
	for attempt := 1; attempt <= 5; attempt++ {
		backoff := CalcJitteredBackoff(attempt, base, max)
		if backoff <= 0 || backoff > max {
			t.Errorf("Attempt %d produced invalid backoff duration: %v", attempt, backoff)
		}
	}
}