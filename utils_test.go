package grabbit

import (
	"testing"
	"time"
)

func TestClampInt(t *testing.T) {
	tests := []struct {
		x     int
		lower int
		upper int
		want  int
	}{
		{x: 5, lower: 1, upper: 10, want: 5},
		{x: 0, lower: 1, upper: 10, want: 1},
		{x: 15, lower: 1, upper: 10, want: 10},
		{x: -5, lower: 1, upper: 10, want: 1},
		{x: 5, lower: -10, upper: 10, want: 5},
		{x: -15, lower: -10, upper: 10, want: -10},
		{x: 15, lower: -10, upper: 10, want: 10},
		{x: -5, lower: -10, upper: -1, want: -5},
		{x: -15, lower: -10, upper: -1, want: -10},
		{x: 0, lower: -10, upper: -1, want: -1},
	}

	for _, tt := range tests {
		got := ClampInt(tt.x, tt.lower, tt.upper)
		if got != tt.want {
			t.Errorf("ClampInt(%d, %d, %d) = %d, want %d", tt.x, tt.lower, tt.upper, got, tt.want)
		}
	}
}

func TestClampInt64(t *testing.T) {
	tests := []struct {
		x     int64
		lower int64
		upper int64
		want  int64
	}{
		{x: 5, lower: 1, upper: 10, want: 5},
		{x: 0, lower: 1, upper: 10, want: 1},
		{x: 15, lower: 1, upper: 10, want: 10},
		{x: -5, lower: 1, upper: 10, want: 1},
		{x: 5, lower: -10, upper: 10, want: 5},
		{x: -15, lower: -10, upper: 10, want: -10},
		{x: 15, lower: -10, upper: 10, want: 10},
		{x: -5, lower: -10, upper: -1, want: -5},
		{x: -15, lower: -10, upper: -1, want: -10},
		{x: 0, lower: -10, upper: -1, want: -1},
	}

	for _, tt := range tests {
		got := ClampInt64(tt.x, tt.lower, tt.upper)
		if got != tt.want {
			t.Errorf("ClampInt64(%d, %d, %d) = %d, want %d", tt.x, tt.lower, tt.upper, got, tt.want)
		}
	}
}

func TestClampDuration(t *testing.T) {
	tests := []struct {
		x     time.Duration
		lower time.Duration
		upper time.Duration
		want  time.Duration
	}{
		{x: 5 * time.Second, lower: 1 * time.Second, upper: 10 * time.Second, want: 5 * time.Second},
		{x: 0 * time.Second, lower: 1 * time.Second, upper: 10 * time.Second, want: 1 * time.Second},
		{x: 15 * time.Second, lower: 1 * time.Second, upper: 10 * time.Second, want: 10 * time.Second},
		{x: -5 * time.Second, lower: 1 * time.Second, upper: 10 * time.Second, want: 1 * time.Second},
		{x: 5 * time.Second, lower: -10 * time.Second, upper: 10 * time.Second, want: 5 * time.Second},
		{x: -15 * time.Second, lower: -10 * time.Second, upper: 10 * time.Second, want: -10 * time.Second},
		{x: 15 * time.Second, lower: -10 * time.Second, upper: 10 * time.Second, want: 10 * time.Second},
		{x: -5 * time.Second, lower: -10 * time.Second, upper: -1 * time.Second, want: -5 * time.Second},
		{x: -15 * time.Second, lower: -10 * time.Second, upper: -1 * time.Second, want: -10 * time.Second},
		{x: 0 * time.Second, lower: -10 * time.Second, upper: -1 * time.Second, want: -1 * time.Second},
	}

	for _, tt := range tests {
		got := ClampDuration(tt.x, tt.lower, tt.upper)
		if got != tt.want {
			t.Errorf("ClampDuration(%v, %v, %v) = %v, want %v", tt.x, tt.lower, tt.upper, got, tt.want)
		}
	}
}
