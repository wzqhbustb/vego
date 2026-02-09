// langgraphdemogo/lance/errors/io.go
package errors

import (
	"os"
)

// FileNotFound 文件不存在
func FileNotFound(path string) error {
	return New(ErrFileNotFound).
		Op("open_file").
		Path(path).
		Severity(SeverityError).
		Build()
}

// PermissionDenied 权限不足
func PermissionDenied(path string, op string) error {
	return New(ErrPermissionDenied).
		Op(op).
		Path(path).
		Severity(SeverityFatal).
		Build()
}

// DiskFull 磁盘满
func DiskFull(path string, required int64) error {
	return New(ErrDiskFull).
		Op("write_file").
		Path(path).
		Context("required_bytes", required).
		Severity(SeverityFatal).
		Build()
}

// ReadAt 读取特定位置失败
func ReadAt(path string, offset int64, size int32, err error) error {
	code := ErrIO
	if os.IsNotExist(err) {
		code = ErrFileNotFound
	}
	return New(code).
		Op("read_at").
		Path(path).
		Offset(offset).
		Context("size", size).
		Wrap(err).
		Build()
}

// WriteAt 写入特定位置失败
func WriteAt(path string, offset int64, size int, err error) error {
	return New(ErrIO).
		Op("write_at").
		Path(path).
		Offset(offset).
		Context("size", size).
		Wrap(err).
		Build()
}

// UnexpectedEOF 意外EOF
func UnexpectedEOF(path string, offset int64, expected, actual int) error {
	return New(ErrUnexpectedEOF).
		Op("read_full").
		Path(path).
		Offset(offset).
		Context("expected_bytes", expected).
		Context("actual_bytes", actual).
		Build()
}
