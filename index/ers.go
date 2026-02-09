package hnsw

import "errors"

var (
	// ErrDimensionMismatch 向量维度不匹配
	ErrDimensionMismatch = errors.New("vector dimension mismatch")

	// ErrEmptyIndex 索引为空
	ErrEmptyIndex = errors.New("index is empty")

	// ErrInvalidParameter 参数无效
	ErrInvalidParameter = errors.New("invalid parameter")
)
