package grabbit

import (
	"context"
	"time"

	latch "github.com/LucaWolf/go-notifying-latch"
)

// ClampInt returns the value of x clamped to the range [lower, upper].
func ClampInt(x, lower, upper int) int {
	return min(max(x, lower), upper)
}

// ClampInt64 returns the value of x clamped to the range [lower, upper].
func ClampInt64(x, lower, upper int64) int64 {
	return min(max(x, lower), upper)
}

// ClampDuration returns the value of x clamped to the range [lower, upper].
func ClampDuration(x, lower, upper time.Duration) time.Duration {
	return time.Duration(min(max(x, lower), upper))
}

// awaitOnLatch helper for awaiting on a particular latch change.
// Latch is locked (true) when is in closed/down (false) state,
// therefore uses negative logic over the passed 'value'.
func awaitOnLatch(
	safety *latch.NotifyingLatch,
	masterCtx context.Context,
	value bool,
	timeout time.Duration,
) bool {
	// zero timeout could take precedence on multiple source select,
	// use the minimum you could afford to waste on negative "immediate test" cases
	if timeout == 0 {
		timeout = 5 * time.Millisecond
	}

	// latch is locked (true) when connection is closed: use negative logic
	wantLocked := !value
	// for the down status the inner CTX could trip before the status change,
	// returning false on the actual latch test, so ignore it
	if wantLocked {
		result := safety.Wait(context.Background(), wantLocked, timeout)
		return result
	} else {
		result := safety.Wait(masterCtx, wantLocked, timeout)
		return result
	}
}
