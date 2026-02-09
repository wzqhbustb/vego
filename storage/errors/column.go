// langgraphdemogo/lance/errors/column.go
package errors

// ColumnNotFound 列未找到
func ColumnNotFound(path string, column string, available []string) error {
	return New(ErrColumnNotFound).
		Op("find_column").
		Path(path).
		Context("column", column).
		Context("available_columns", available).
		Build()
}

// PageNotFound 页未找到
func PageNotFound(path string, columnIdx, pageNum int32) error {
	return New(ErrPageNotFound).
		Op("get_page").
		Path(path).
		Context("column_index", columnIdx).
		Context("page_number", pageNum).
		Build()
}

// TypeMismatch 类型不匹配
func TypeMismatch(op string, column string, expected, actual string) error {
	return New(ErrTypeMismatch).
		Op(op).
		Context("column", column).
		Context("expected_type", expected).
		Context("actual_type", actual).
		Build()
}

// BufferTooSmall 缓冲区太小
func BufferTooSmall(op string, required, available int) error {
	return New(ErrBufferTooSmall).
		Op(op).
		Context("required_bytes", required).
		Context("available_bytes", available).
		Build()
}

// ValidationFailed 验证失败
func ValidationFailed(op string, path string, details string) error {
	return New(ErrInvalidArgument).
		Op(op).
		Path(path).
		Context("validation_error", details).
		Build()
}
