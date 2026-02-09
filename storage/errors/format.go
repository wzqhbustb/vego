// langgraphdemogo/lance/errors/format.go
package errors

import "fmt"

// FormatInvalidMagic Magic号不匹配
func FormatInvalidMagic(path string, got, want uint32) error {
	return New(ErrInvalidMagic).
		Op("validate_header").
		Path(path).
		Context("got", fmt.Sprintf("0x%08X", got)).
		Context("want", fmt.Sprintf("0x%08X", want)).
		Build()
}

// FormatVersionMismatch 版本不匹配
func FormatVersionMismatch(path string, got, min, max uint16) error {
	return New(ErrVersionMismatch).
		Op("validate_version").
		Path(path).
		Context("version", got).
		Context("min_supported", min).
		Context("max_supported", max).
		Build()
}

// FormatCorrupted 文件损坏
func FormatCorrupted(path string, offset int64, reason string) error {
	return New(ErrCorruptedFile).
		Op("read_data").
		Path(path).
		Offset(offset).
		Context("reason", reason).
		Severity(SeverityFatal).
		Build()
}

// SchemaMismatch Schema不匹配
func SchemaMismatch(path string, field string, expected, actual string) error {
	return New(ErrSchemaMismatch).
		Op("validate_schema").
		Path(path).
		Context("field", field).
		Context("expected_type", expected).
		Context("actual_type", actual).
		Build()
}

// MetadataError 元数据错误
func MetadataError(op, path string, field string, err error) error {
	return New(ErrMetadataError).
		Op(op).
		Path(path).
		Context("field", field).
		Wrap(err).
		Build()
}
