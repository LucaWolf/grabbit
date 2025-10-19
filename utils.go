package grabbit

import "time"

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
