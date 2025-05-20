package traceutils

import (
	"context"
	"fmt"
	"reflect"
)

type ParamsTrace struct {
	Name  string // kind of params
	Tag   string // instance of same params
	Value any    // any of the Params<Name> struct
}

func (p ParamsTrace) String() string {
	return p.Name + "." + p.Tag
}

type ErrorTrace struct {
	Name string // kind of params
	Tag  string // instance of same params
	Err  error
}

type FieldExpectation struct {
	Field string // name of field (in ParamsTrace.Value) to test
	Value reflect.Value
}

type TraceValidator struct {
	Name         string // kind of params
	Tag          string // instance of same params
	Expectations []FieldExpectation
}

var (
	ParamsCh  chan ParamsTrace
	ResultsCh chan ErrorTrace
)

// ConsumeTraces sets ParamsCh and ResultsCh and starts a ParamsTrace loop processing goroutine.
// Call it directly from your tests without a 'go' wrapper.
func ConsumeTraces(ctx context.Context, validators []TraceValidator) {
	// wrapper setup to avoid race condition on channels initialization (read before write/make)
	ParamsCh = make(chan ParamsTrace, 32)
	ResultsCh = make(chan ErrorTrace, 32)

	go doConsumeTraces(ctx, validators)
}

func doConsumeTraces(ctx context.Context, validators []TraceValidator) {
	for {
		select {
		case trace := <-ParamsCh:
			var expected []FieldExpectation
			for _, v := range validators {
				if v.Name == trace.Name && v.Tag == trace.Tag {
					expected = v.Expectations
					break
				}
			}
			params := reflect.ValueOf(trace.Value)
			for _, e := range expected {
				param := params.FieldByName(e.Field)
				if !param.IsValid() {
					ResultsCh <- ErrorTrace{
						Name: trace.Name,
						Tag:  trace.Tag,
						Err:  fmt.Errorf("field %s not found. Adjust your FieldExpectation(s)", e.Field),
					}
					continue
				}
				// maps are not comparable and require special handling
				if param.Kind() == reflect.Map {
					if !reflect.DeepEqual(param.Interface(), e.Value.Interface()) {
						ResultsCh <- ErrorTrace{
							Name: trace.Name,
							Tag:  trace.Tag,
							Err: fmt.Errorf(
								"field %s mismatch: Expected %#v, got %#v",
								e.Field,
								e.Value,
								param,
							),
						}
					}
					continue
				}
				if !param.Equal(e.Value) {
					ResultsCh <- ErrorTrace{
						Name: trace.Name,
						Tag:  trace.Tag,
						Err: fmt.Errorf(
							"field %s mismatch: Expected %#v, got %#v",
							e.Field,
							e.Value,
							param,
						),
					}
				}
			}
		case <-ctx.Done():
			return
		}
	}
}
