package grabbit

import (
	"testing"

	latch "github.com/LucaWolf/go-notifying-latch"
)

// TestLatchTransitions pins the go-notifying-latch transition-counter semantics
// the concurrent settle path leans on. It is deliberately latch-only (no broker):
// the counter is dependency-internal, so its behavior must be recorded here before
// production code compares snapshots.
func TestLatchTransitions(t *testing.T) {
	const tag = "latch.pin"

	l := latch.NewLatch(true, tag)

	// Freshly-created locked latch: no transition has happened yet.
	if gotTag, transitions := l.Details(); gotTag != tag || transitions != 0 {
		t.Fatalf("fresh latch Details() = (%q, %d), want (%q, 0)", gotTag, transitions, tag)
	}

	// Establishment Unlock counts as a transition.
	l.Unlock()
	if _, transitions := l.Details(); transitions != 1 {
		t.Fatalf("after establishment Unlock: transitions = %d, want 1", transitions)
	}

	// Recovery start (Lock) and completion (Unlock) each count.
	l.Lock()
	if _, transitions := l.Details(); transitions != 2 {
		t.Fatalf("after recovery Lock: transitions = %d, want 2", transitions)
	}
	l.Unlock()
	if _, transitions := l.Details(); transitions != 3 {
		t.Fatalf("after recovery Unlock: transitions = %d, want 3", transitions)
	}

	// Idempotent calls do not count: the counter tracks real status changes only.
	l.Unlock()
	l.Unlock()
	if _, transitions := l.Details(); transitions != 3 {
		t.Fatalf("idempotent Unlock changed transitions to %d, want 3", transitions)
	}
	l.Lock()
	l.Lock()
	if _, transitions := l.Details(); transitions != 4 {
		t.Fatalf("idempotent Lock changed transitions to %d, want 4", transitions)
	}
}

// TestLatchTransitionsInitiallyUnlocked pins the other constructor branch: an
// unlocked latch starts at zero transitions and its first Lock counts.
func TestLatchTransitionsInitiallyUnlocked(t *testing.T) {
	l := latch.NewLatch(false, "latch.pin.open")
	if _, transitions := l.Details(); transitions != 0 {
		t.Fatalf("fresh unlocked latch transitions = %d, want 0", transitions)
	}
	l.Lock()
	if _, transitions := l.Details(); transitions != 1 {
		t.Fatalf("first Lock transitions = %d, want 1", transitions)
	}
}
