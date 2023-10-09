package grabbit

import (
	"errors"
)

type OptionalError struct {
	err   error
	isSet bool
}

// SomeErrFromError creates an OptionalError struct with the given error and isSet values.
//
// Parameters:
// - err: The error to be assigned to the OptionalError struct.
// - isSet: A boolean value indicating whether the error is set or not.
//
// Return:
// - OptionalError: The OptionalError struct with the assigned error and isSet values.
func SomeErrFromError(err error, isSet bool) OptionalError {
	return OptionalError{
		// dereferencing an interface always gets us a valid address.
		// nil testing of this pointer won't work
		err:   err,
		isSet: isSet,
	}
}

// SomeErrFromString creates an OptionalError from the specified text.
//
// Parameters:
// - text: the string to create the error from.
//
// Return type:
// - OptionalError: the created OptionalError.
func SomeErrFromString(text string) OptionalError {
	err := errors.New(text)
	return OptionalError{
		err:   err,
		isSet: true,
	}
}

// Or returns the optional error if it is set, otherwise it returns the provided error.
//
// err - The error to return if the optional error is not set.
// error - The optional error.
func (e OptionalError) Or(err error) error {
	if e.isSet {
		return e.err
	}
	return err
}

// Error returns the error string representation of the OptionalError.
//
// It calls the Or method on the OptionalError to get the error value and returns its
// Error method.
func (e OptionalError) Error() string {
	err := e.Or(errors.New("no error"))
	return err.Error()
}

// IsSet returns a boolean value indicating whether the OptionalError is set.
//
// This function does not take any parameters.
// It returns a boolean value.
func (e OptionalError) IsSet() bool {
	return e.isSet
}
