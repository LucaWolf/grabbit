package notifiers

import (
	"context"
	"sync"
	"time"
)

// SafeNotifiers wraps in a concurrency safe way the notification channel and status.
type SafeNotifiers struct {
	channel chan struct{} // notifies consumers of events
	mu      sync.RWMutex  // protects channel and version
	status  bool          // subject of transition to true notification
	tag     string        // helper for tracing
}

// NewSafeNotifiers creates a new SafeNotifiers struct.
func NewSafeNotifiers(tag string) SafeNotifiers {
	return SafeNotifiers{
		channel: make(chan struct{}),
		status:  false,
		tag:     tag,
	}
}

// NextVersion increases the notifier's status in preparation for AwaitFor to block on channel events.
func (s *SafeNotifiers) Reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.status = false
}

// Broadcast closes the existing channel and creates a new one, usually follows status.
func (s *SafeNotifiers) Broadcast() {
	s.mu.Lock()
	defer s.mu.Unlock()

	close(s.channel)
	s.channel = make(chan struct{})
	s.status = true
}

// Channel returns the notification channel and status.
func (s *SafeNotifiers) Channel() (<-chan struct{}, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.channel, s.status
}

// Status returns the current status.
func (s *SafeNotifiers) Status() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.status
}

// AwaitFor polls for either the channel closed event or timeout expiry.
func (s *SafeNotifiers) AwaitFor(ctx context.Context, timeout time.Duration) bool {
	ch, status := s.Channel() // safe read of the latest knonw snap-shot for ch polling

	if !status {
		select {
		case <-ch:
			return true
		case <-time.After(timeout):
			return false
		case <-ctx.Done():
			return false
		}
	}
	return status
}
