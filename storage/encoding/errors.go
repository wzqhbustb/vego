package encoding

import (
	lerrors "github.com/wzqhbustb/vego/storage/errors"
)

var (
	// ErrNullNotSupported indicates the encoder cannot handle null values.
	// PageWriter should catch this and fall back to Zstd.
	ErrNullNotSupported = lerrors.New(lerrors.ErrNullNotSupported).
		Op("encode").
		Build()

	// ErrUnsupportedType indicates the encoder doesn't support this data type.
	ErrUnsupportedType = lerrors.New(lerrors.ErrUnsupportedType).
		Op("encode").
		Build()

	// ErrEmptyArray indicates the array is empty.
	ErrEmptyArray = lerrors.New(lerrors.ErrInvalidArgument).
		Op("encode").
		Context("reason", "empty array").
		Build()
)

// EncodeError creates a structured encoding error
func EncodeError(encoding string, op string, err error) error {
	return lerrors.New(lerrors.ErrEncodeFailed).
		Op(op).
		Context("encoding", encoding).
		Wrap(err).
		Build()
}

// DecodeError creates a structured decoding error
func DecodeError(encoding string, op string, err error) error {
	return lerrors.New(lerrors.ErrDecodeFailed).
		Op(op).
		Context("encoding", encoding).
		Wrap(err).
		Build()
}

// IsNullUnsupportedError checks if an error indicates that the encoder
// cannot handle null values and should fall back to another encoder.
func IsNullUnsupportedError(err error) bool {
	if err == nil {
		return false
	}
	return lerrors.Is(err, lerrors.ErrNullNotSupported)
}
