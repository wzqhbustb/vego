package encoding

import "github.com/wzqhbustb/vego/core"

var (
	// ErrNullNotSupported indicates the encoder cannot handle null values.
	// PageWriter should catch this and fall back to Zstd.
	ErrNullNotSupported = core.New(core.ErrNullNotSupported).
				Op("encode").
				Build()

	// ErrUnsupportedType indicates the encoder doesn't support this data type.
	ErrUnsupportedType = core.New(core.ErrUnsupportedType).
				Op("encode").
				Build()

	// ErrEmptyArray indicates the array is empty.
	ErrEmptyArray = core.New(core.ErrInvalidArgument).
			Op("encode").
			Context("reason", "empty array").
			Build()

	// ErrInvalidData indicates the encoded data is invalid or corrupted.
	ErrInvalidData = core.New(core.ErrInvalidArgument).
			Op("encode").
			Context("reason", "invalid data").
			Build()

	// ErrCorruptedNullBitmap indicates the null bitmap doesn't match the values (data corruption).
	ErrCorruptedNullBitmap = core.New(core.ErrCorruptedFile).
				Op("expand_nulls").
				Context("reason", "null bitmap count mismatch").
				Build()
)

// EncodeError creates a structured encoding error
func EncodeError(encoding string, op string, err error) error {
	return core.New(core.ErrEncodeFailed).
		Op(op).
		Context("encoding", encoding).
		Wrap(err).
		Build()
}

// DecodeError creates a structured decoding error
func DecodeError(encoding string, op string, err error) error {
	return core.New(core.ErrDecodeFailed).
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
	return core.Is(err, core.ErrNullNotSupported)
}
