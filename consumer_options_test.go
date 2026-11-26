package grabbit

import "testing"

// TestConsumerOptionsProcessorConcurrencyDefault pins the zero value: the
// sequential batch path must remain the default.
func TestConsumerOptionsProcessorConcurrencyDefault(t *testing.T) {
	opt := DefaultConsumerOptions()
	if opt.ProcessorConcurrency != 0 {
		t.Errorf("default ProcessorConcurrency = %d, want 0", opt.ProcessorConcurrency)
	}
}

// TestConsumerOptionsWithProcessorConcurrency pins the builder's fluent return
// and the stored value, including the documented negative-means-off case.
func TestConsumerOptionsWithProcessorConcurrency(t *testing.T) {
	opt := DefaultConsumerOptions()

	got := opt.WithProcessorConcurrency(8)
	if got != &opt {
		t.Error("WithProcessorConcurrency should return the receiver")
	}
	if opt.ProcessorConcurrency != 8 {
		t.Errorf("ProcessorConcurrency = %d, want 8", opt.ProcessorConcurrency)
	}

	opt.WithProcessorConcurrency(-1)
	if opt.ProcessorConcurrency != -1 {
		t.Errorf("ProcessorConcurrency = %d, want -1", opt.ProcessorConcurrency)
	}
}
