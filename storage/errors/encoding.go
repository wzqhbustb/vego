// langgraphdemogo/lance/errors/encoding.go
package errors

import "fmt"

// EncodeFailed 编码失败
func EncodeFailed(encoder string, dataType string, err error) error {
	return New(ErrEncodeFailed).
		Op(fmt.Sprintf("encode_%s", encoder)).
		Context("encoder", encoder).
		Context("data_type", dataType).
		Wrap(err).
		Build()
}

// DecodeFailed 解码失败
func DecodeFailed(encoder string, dataType string, offset int64, reason string) error {
	return New(ErrDecodeFailed).
		Op(fmt.Sprintf("decode_%s", encoder)).
		Offset(offset).
		Context("encoder", encoder).
		Context("data_type", dataType).
		Context("reason", reason).
		Build()
}

// UnsupportedType 不支持的类型
func UnsupportedType(op string, dataType string, encoder string) error {
	return New(ErrUnsupportedType).
		Op(op).
		Context("data_type", dataType).
		Context("encoder", encoder).
		Context("message", fmt.Sprintf("encoder %s does not support type %s", encoder, dataType)).
		Build()
}

// CompressionFailed 压缩失败
func CompressionFailed(codec string, inputSize int, err error) error {
	return New(ErrCompressionFailed).
		Op(fmt.Sprintf("%s_compress", codec)).
		Context("codec", codec).
		Context("input_size", inputSize).
		Wrap(err).
		Build()
}

// NullNotSupported 编码器不支持null
func NullNotSupported(encoder string, dataType string) error {
	return New(ErrNullNotSupported).
		Op(fmt.Sprintf("encode_%s", encoder)).
		Context("encoder", encoder).
		Context("data_type", dataType).
		Context("message", "this encoder does not support null values, consider using Zstd instead").
		Build()
}

// DecodeSizeMismatch 解码后大小不匹配
func DecodeSizeMismatch(encoder string, expected, actual int) error {
	return New(ErrDecodeFailed).
		Op(fmt.Sprintf("decode_%s", encoder)).
		Context("expected_values", expected).
		Context("actual_values", actual).
		Context("reason", "size mismatch after decoding").
		Build()
}
