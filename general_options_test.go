package grabbit

import (
	"testing"
	"time"
)

func TestDefaultDelayer_Delay(t *testing.T) {
	delayer := DefaultDelayer{Value: 1 * time.Second, RetryCap: 7}

	testCases := []struct {
		retry    int
		expected time.Duration
	}{
		{retry: -1, expected: 1 * time.Second}, // retry is internally fixed/clamped
		{retry: 0, expected: 1 * time.Second},  // retry is internally fixed/clamped
		{retry: 1, expected: 1 * time.Second},
		{retry: 2, expected: 2 * time.Second},
		{retry: 3, expected: 4 * time.Second},
		{retry: 4, expected: 8 * time.Second},
		{retry: 5, expected: 16 * time.Second},
		{retry: 6, expected: 32 * time.Second},
		{retry: 7, expected: 64 * time.Second},
		{retry: 8, expected: 64 * time.Second},  // should be capped at 7
		{retry: 32, expected: 64 * time.Second}, // should be capped at 7
	}

	for _, tc := range testCases {
		delay := delayer.Delay(tc.retry)
		// Allow for some jitter in the expected value
		if float64(delay) < float64(tc.expected)*0.8 || float64(delay) > float64(tc.expected)*1.2 {
			t.Errorf("Retry: %d, Expected delay: %v, Got: %v", tc.retry, tc.expected, delay)
		}
	}
}
