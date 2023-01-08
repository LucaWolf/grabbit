package grabbit

import (
	"errors"
)

type OptionalError struct {
	err   *error
	isSet bool
}

func SomeErrFromError(err error, isSet bool) OptionalError {
	return OptionalError{
		// dereferencing an interface always gets us a valid address.
		// nil testing of this pointer won't work
		err:   &err,
		isSet: isSet,
	}
}

func SomeErrFromString(text string) OptionalError {
	err := errors.New(text)
	return OptionalError{
		err:   &err,
		isSet: true,
	}
}

func (e OptionalError) Or(err error) error {
	if e.isSet {
		return *e.err
	}
	return err
}

func (e OptionalError) Error() string {
	err := e.Or(errors.New("no error"))
	return err.Error()
}

func (e OptionalError) IsSet() bool {
	return e.isSet
}
