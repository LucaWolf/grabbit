package grabbit

import (
	"errors"
	"testing"
)

func TestSomeErrFromError(t *testing.T) {
	type args struct {
		err   error
		isSet bool
	}
	tests := []struct {
		name string
		args args
		want OptionalError
	}{
		{
			"blank set", // invalid built combination but we test for inner fields
			args{err: nil, isSet: true},
			OptionalError{err: nil, isSet: true},
		},
		{
			"blank unset",
			args{err: nil, isSet: false},
			OptionalError{err: nil, isSet: false},
		},
		{
			"set value matched",
			args{err: errors.New("some_error"), isSet: true},
			OptionalError{err: errors.New("some_error"), isSet: true},
		},
		{
			"unset value ignored",
			args{err: errors.New("test"), isSet: false},
			OptionalError{err: errors.New("test-boo"), isSet: false},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := SomeErrFromError(tt.args.err, tt.args.isSet)
			if got.isSet != tt.want.isSet {
				t.Errorf("SomeErrFromError().set = %v, want %v", got, tt.want)
			}
			if got.err != nil && tt.want.err == nil {
				t.Errorf("SomeErrFromError().nil = output err should exists")
			}
			if got.err == nil && tt.want.err != nil {
				t.Errorf("SomeErrFromError().nil = output err should be nil")
			}
			if got.err != nil && tt.want.err != nil {
				if got.Error() != tt.want.Error() {
					t.Errorf("SomeErrFromError().Error() = %v, want %v", got.Error(), tt.want.Error())
				}
			}

		})
	}
}

func TestSomeErrFromString(t *testing.T) {
	type args struct {
		text string
	}
	tests := []struct {
		name string
		args args
		want OptionalError
	}{
		{
			"blank value",
			args{text: ""},
			OptionalError{err: errors.New(""), isSet: true},
		},
		{
			"value",
			args{text: "something went wrong"},
			OptionalError{err: errors.New("something went wrong"), isSet: true},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := SomeErrFromString(tt.args.text)
			if !got.isSet || got.Error() != tt.want.Error() {
				t.Errorf("SomeErrFromString() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestOptionalError_Or(t *testing.T) {
	type fields struct {
		err   error
		isSet bool
	}
	type args struct {
		err error
	}
	tests := []struct {
		name      string
		fields    fields
		args      args
		wantErr   bool
		wantValue string
	}{
		{
			"unset nil, no alternative error",
			fields{err: nil, isSet: false},
			args{err: nil},
			false,
			"",
		},
		{
			"set nil, no alternative error",
			fields{err: nil, isSet: true},
			args{err: nil},
			false,
			"",
		},
		{
			"set value, no alternative error",
			fields{err: errors.New("some_error_a"), isSet: true},
			args{err: nil},
			true,
			"some_error_a",
		},
		{
			"unset value, no alternative error",
			fields{err: errors.New("some_error"), isSet: false},
			args{err: nil},
			false,
			"",
		},
		//
		{
			"unset nil, alternative error",
			fields{err: nil, isSet: false},
			args{err: errors.New("some_error_b")},
			true,
			"some_error_b", // original unset, use alternative
		},
		{
			"set nil, alternative error",
			fields{err: nil, isSet: true},
			args{err: errors.New("some_error_c")},
			true,
			"some_error_c", // original sest but nil, use alternative
		},
		{
			"set value, alternative error",
			fields{err: errors.New("some_error_d"), isSet: true},
			args{err: errors.New("some_error_dd")},
			true,
			"some_error_d", // original set, use it
		},
		{
			"unset value, alternative error",
			fields{err: errors.New("some_error_e"), isSet: false},
			args{err: errors.New("some_error_ee")},
			true,
			"some_error_ee", // original unset, use alternative
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := OptionalError{
				err:   tt.fields.err,
				isSet: tt.fields.isSet,
			}
			err := e.Or(tt.args.err)
			if (err != nil) != tt.wantErr {
				t.Errorf("OptionalError.Or() error = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.wantErr && err.Error() != tt.wantValue {
				t.Errorf("OptionalError.Or().Value = %v, want %v", err.Error(), tt.wantValue)
			}
		})
	}
}

func TestOptionalError_Error(t *testing.T) {
	type fields struct {
		err   error
		isSet bool
	}
	tests := []struct {
		name   string
		fields fields
		want   string
	}{
		{
			"unset nil",
			fields{err: nil, isSet: false},
			"no error",
		},
		{
			"set nil",
			fields{err: nil, isSet: true},
			"no error",
		},
		{
			"set value",
			fields{err: errors.New("some_error"), isSet: true},
			"some_error",
		},
		{
			"unset value",
			fields{err: errors.New("some_error"), isSet: false},
			"no error",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := OptionalError{
				err:   tt.fields.err,
				isSet: tt.fields.isSet,
			}
			if got := e.Error(); got != tt.want {
				t.Errorf("OptionalError.Error() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestOptionalError_IsSet(t *testing.T) {
	type fields struct {
		err   error
		isSet bool
	}
	tests := []struct {
		name   string
		fields fields
		want   bool
	}{
		{
			"unset nil",
			fields{err: nil, isSet: false},
			false,
		},
		{
			"set nil",
			fields{err: nil, isSet: true},
			false,
		},
		{
			"set value",
			fields{err: errors.New("some_error"), isSet: true},
			true,
		},
		{
			"unset value",
			fields{err: errors.New("some_error"), isSet: false},
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := OptionalError{
				err:   tt.fields.err,
				isSet: tt.fields.isSet,
			}
			if got := e.IsSet(); got != tt.want {
				t.Errorf("OptionalError.IsSet() = %v, want %v", got, tt.want)
			}
		})
	}
}
