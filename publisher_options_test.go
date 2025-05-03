package grabbit

import (
	"context"
	"reflect"
	"testing"
)

func TestDefaultPublisherOptions(t *testing.T) {
	ctx := context.TODO()
	opt := PublisherOptions{
		PublisherUsageOptions: PublisherUsageOptions{
			ConfirmationCount:  10,
			ConfirmationNoWait: false,
			IsPublisher:        true,
		},
		Immediate: false,
		Mandatory: false,
	}

	tests := []struct {
		name string
		want PublisherOptions
	}{
		{
			"default",
			*opt.WithContext(ctx),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := DefaultPublisherOptions(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("DefaultPublisherOptions() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestPublisherOptions_Various(t *testing.T) {
	opt := DefaultPublisherOptions()

	// ConfirmationNoWait
	if opt.ConfirmationNoWait != false {
		t.Errorf("DefaultPublisherOptions().ConfirmationNoWait -- should default to false")
	}

	result := opt.WithConfirmationNoWait(true)
	if result.ConfirmationNoWait != true {
		t.Errorf("opt.WithConfirmationNoWait(true).ConfirmationNoWait -- should be true")
	}

	result = opt.WithConfirmationNoWait(false)
	if result.ConfirmationNoWait != false {
		t.Errorf("opt.WithConfirmationNoWait(false).ConfirmationNoWait -- should be false")
	}

	// WithContext
	ctx := context.TODO()
	result = opt.WithContext(ctx)
	if result.Context != ctx {
		t.Errorf("opt.WithContext(ctx).Context -- should be ctx")
	}

	// WithImmediate
	result = opt.WithImmediate(true)
	if result.Immediate != true {
		t.Errorf("opt.WithImmediate(true).Immediate -- should be true")
	}
	result = opt.WithImmediate(false)
	if result.Immediate != false {
		t.Errorf("opt.WithImmediate(false).Immediate -- should be false")
	}

	// WithMandatory
	result = opt.WithMandatory(true)
	if result.Mandatory != true {
		t.Errorf("opt.WithMandatory(true).Mandatory -- should be true")
	}
	result = opt.WithMandatory(false)
	if result.Mandatory != false {
		t.Errorf("opt.WithMandatory(false).Mandatory -- should be false")
	}

	// WithExchange
	result = opt.WithExchange("test")
	if result.Exchange != "test" {
		t.Errorf("opt.WithExchange(\"test\").Exchange -- should be \"test\"")
	}

	// WithKey
	result = opt.WithKey("test")
	if result.Key != "test" {
		t.Errorf("opt.WithKey(\"test\").Key -- should be \"test\"")
	}

	// WithConfirmationCount
	result = opt.WithConfirmationsCount(15)
	if result.ConfirmationCount != 15 {
		t.Errorf("opt.WithConfirmationCount(15).ConfirmationCount -- should be 15")
	}

	if !reflect.DeepEqual(result, &opt) {
		t.Errorf("DefaultPublisherOptions().result.pointer = %v, want %v", result, opt)
	}
}
