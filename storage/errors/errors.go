// langgraphdemogo/lance/errors/errors.go
package errors

import (
	"errors"
	"fmt"
	"io"
	"runtime/debug"
)

// ErrorCode 错误分类码
type ErrorCode int

const (
	// 通用错误 (1-99)
	ErrUnknown ErrorCode = iota
	ErrInvalidArgument
	ErrNotSupported
	ErrOutOfMemory
	ErrCancelled
	ErrTimeout

	// 文件格式错误 (100-199)
	ErrInvalidMagic
	ErrVersionMismatch
	ErrCorruptedFile
	ErrSchemaMismatch
	ErrMetadataError

	// 编码错误 (200-299)
	ErrEncodeFailed
	ErrDecodeFailed
	ErrUnsupportedType
	ErrCompressionFailed
	ErrNullNotSupported

	// I/O 错误 (300-399)
	ErrIO
	ErrFileNotFound
	ErrPermissionDenied
	ErrDiskFull
	ErrBrokenPipe
	ErrUnexpectedEOF

	// 列操作错误 (400-499)
	ErrColumnNotFound
	ErrPageNotFound
	ErrTypeMismatch
	ErrBufferTooSmall
)

func (c ErrorCode) String() string {
	switch c {
	case ErrUnknown:
		return "Unknown"
	case ErrInvalidArgument:
		return "InvalidArgument"
	case ErrNotSupported:
		return "NotSupported"
	case ErrInvalidMagic:
		return "InvalidMagic"
	case ErrVersionMismatch:
		return "VersionMismatch"
	case ErrCorruptedFile:
		return "CorruptedFile"
	case ErrEncodeFailed:
		return "EncodeFailed"
	case ErrDecodeFailed:
		return "DecodeFailed"
	case ErrIO:
		return "IO"
	case ErrFileNotFound:
		return "FileNotFound"
	case ErrColumnNotFound:
		return "ColumnNotFound"
	// ... 其他映射
	default:
		return fmt.Sprintf("ErrorCode(%d)", c)
	}
}

// ErrorSeverity 错误严重程度
type ErrorSeverity int

const (
	SeverityWarning ErrorSeverity = iota // 可恢复，可忽略
	SeverityError                        // 操作失败，但系统稳定
	SeverityFatal                        // 系统状态可能不一致，需要重启/清理
)

// LanceError 基础错误结构
type LanceError struct {
	Code     ErrorCode              // 错误码
	Severity ErrorSeverity          // 严重程度
	Op       string                 // 操作描述（如 "read page", "encode int32 array"）
	Path     string                 // 关联文件路径
	Offset   int64                  // 文件偏移（如果有）
	Err      error                  // 原始错误（错误链）
	Context  map[string]interface{} // 额外上下文
	Stack    []byte                 // 堆栈信息（调试模式）
}

func (e *LanceError) Error() string {
	var parts []string

	// 基础信息
	parts = append(parts, fmt.Sprintf("[%s:%s]", e.Code, e.Op))

	// 位置信息
	if e.Path != "" {
		if e.Offset >= 0 {
			parts = append(parts, fmt.Sprintf("path=%s offset=%d", e.Path, e.Offset))
		} else {
			parts = append(parts, fmt.Sprintf("path=%s", e.Path))
		}
	}

	// 上下文
	if len(e.Context) > 0 {
		parts = append(parts, fmt.Sprintf("context=%v", e.Context))
	}

	// 原始错误
	if e.Err != nil {
		parts = append(parts, fmt.Sprintf("cause=%v", e.Err))
	}

	return fmt.Sprintf("lance error: %s", joinParts(parts))
}

func joinParts(parts []string) string {
	if len(parts) == 0 {
		return "unknown"
	}
	result := parts[0]
	for _, p := range parts[1:] {
		result += " | " + p
	}
	return result
}

// Unwrap 支持 errors.As/Is
func (e *LanceError) Unwrap() error {
	return e.Err
}

// IsCode 判断错误码是否匹配
func (e *LanceError) IsCode(code ErrorCode) bool {
	return e.Code == code
}

// WithContext 添加上下文（链式调用）
func (e *LanceError) WithContext(key string, value interface{}) *LanceError {
	if e.Context == nil {
		e.Context = make(map[string]interface{})
	}
	e.Context[key] = value
	return e
}

// WithOffset 设置偏移（链式调用）
func (e *LanceError) WithOffset(offset int64) *LanceError {
	e.Offset = offset
	return e
}

// Builder 模式构造错误
type ErrorBuilder struct {
	err *LanceError
}

func New(code ErrorCode) *ErrorBuilder {
	return &ErrorBuilder{
		err: &LanceError{
			Code:     code,
			Severity: SeverityError,
			Offset:   -1,
			Context:  make(map[string]interface{}),
		},
	}
}

func (b *ErrorBuilder) Op(op string) *ErrorBuilder {
	b.err.Op = op
	return b
}

func (b *ErrorBuilder) Path(path string) *ErrorBuilder {
	b.err.Path = path
	return b
}

func (b *ErrorBuilder) Offset(offset int64) *ErrorBuilder {
	b.err.Offset = offset
	return b
}

func (b *ErrorBuilder) Wrap(err error) *ErrorBuilder {
	b.err.Err = err
	return b
}

func (b *ErrorBuilder) Severity(s ErrorSeverity) *ErrorBuilder {
	b.err.Severity = s
	return b
}

func (b *ErrorBuilder) Context(key string, value interface{}) *ErrorBuilder {
	b.err.Context[key] = value
	return b
}

func (b *ErrorBuilder) WithStack() *ErrorBuilder {
	b.err.Stack = debug.Stack()
	return b
}

func (b *ErrorBuilder) Build() error {
	return b.err
}

// 便捷构造函数

// Unknown 创建未知错误
func Unknown(op string, err error) error {
	return New(ErrUnknown).Op(op).Wrap(err).Build()
}

// InvalidArg 创建参数错误
func InvalidArg(op string, msg string) error {
	return New(ErrInvalidArgument).Op(op).Context("message", msg).Build()
}

// IO 创建IO错误
func IO(op string, path string, err error) error {
	code := ErrIO
	if errors.Is(err, io.ErrUnexpectedEOF) {
		code = ErrUnexpectedEOF
	}
	return New(code).Op(op).Path(path).Wrap(err).Build()
}

// Is 判断错误是否属于某类错误码
func Is(err error, code ErrorCode) bool {
	if err == nil {
		return false
	}

	// 检查当前错误
	var le *LanceError
	if errors.As(err, &le) {
		if le.Code == code {
			return true
		}
		// 递归检查 cause
		if le.Err != nil {
			return Is(le.Err, code)
		}
	}

	return false
}

// IsAny 判断是否属于任何一类错误码
func IsAny(err error, codes ...ErrorCode) bool {
	for _, code := range codes {
		if Is(err, code) {
			return true
		}
	}
	return false
}

// IsRecoverable 判断错误是否可恢复
func IsRecoverable(err error) bool {
	var le *LanceError
	if errors.As(err, &le) {
		return le.Severity == SeverityWarning || le.Severity == SeverityError
	}
	return false
}

// IsFatal 判断错误是否致命
func IsFatal(err error) bool {
	var le *LanceError
	if errors.As(err, &le) {
		return le.Severity == SeverityFatal
	}
	return false
}

// GetCode 获取错误码（如果不是LanceError返回ErrUnknown）
func GetCode(err error) ErrorCode {
	var le *LanceError
	if errors.As(err, &le) {
		return le.Code
	}
	return ErrUnknown
}
