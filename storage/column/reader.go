package column

import (
	"context"
	"fmt"
	"hash/fnv"
	"io"
	"github.com/wzqhbkjdx/vego/storage/arrow"
	lerrors "github.com/wzqhbkjdx/vego/storage/errors"
	"github.com/wzqhbkjdx/vego/storage/format"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	lanceio "github.com/wzqhbkjdx/vego/storage/io" // 使用别名避免冲突
)

// Reader reads RecordBatch data from a Lance file
type Reader struct {
	file       *os.File
	header     *format.Header
	footer     *format.Footer
	pageReader *PageReader
	closed     bool
	mu         sync.Mutex

	// Phase 2: 异步 I/O 支持（可选）
	asyncIO      *lanceio.AsyncIO
	fileID       string // 在 AsyncIO 中注册的文件 ID
	useAsync     bool   // 是否启用异步模式
	asyncEnabled bool   // AsyncIO 是否可用（文件已注册）
}

// NewReader creates a new column reader（同步模式）
func NewReader(filename string) (*Reader, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, lerrors.IO("new_reader", filename, err)
	}

	reader := &Reader{
		file:       file,
		pageReader: NewPageReader(),
		closed:     false,
		useAsync:   false, // 默认同步模式
	}

	// Read header
	if err := reader.readHeader(); err != nil {
		file.Close()
		return nil, lerrors.New(lerrors.ErrCorruptedFile).
			Op("read_header").
			Context("message", "read header failed").
			Wrap(err).
			Build()
	}

	// Read footer
	if err := reader.readFooter(); err != nil {
		file.Close()
		return nil, lerrors.New(lerrors.ErrCorruptedFile).
			Op("read_footer").
			Context("message", "read footer failed").
			Wrap(err).
			Build()
	}

	return reader, nil
}

// NewReaderWithAsyncIO 不需要自己打开文件
func NewReaderWithAsyncIO(filename string, asyncIO *lanceio.AsyncIO) (*Reader, error) {
	if asyncIO == nil {
		return NewReader(filename)
	}

	fileID := generateFileID(filename)

	// 1. 注册文件到 AsyncIO/FilePool
	if err := asyncIO.RegisterFile(fileID, filename); err != nil {
		return nil, lerrors.New(lerrors.ErrIO).
			Op("register_file_async").
			Context("file_id", fileID).
			Wrap(err).
			Build()
	}

	// 2. 获取文件句柄（增加引用计数）
	file, err := asyncIO.GetFile(fileID)
	if err != nil {
		return nil, lerrors.New(lerrors.ErrIO).
			Op("get_file_async").
			Context("file_id", fileID).
			Wrap(err).
			Build()
	}

	reader := &Reader{
		file:         file, // 使用 FilePool 管理的句柄
		pageReader:   NewPageReader(),
		closed:       false,
		asyncIO:      asyncIO,
		fileID:       fileID,
		useAsync:     true,
		asyncEnabled: true,
	}

	// 读取 header/footer（使用 FilePool 的句柄）
	if err := reader.readHeader(); err != nil {
		asyncIO.ReleaseFile(fileID) // 清理
		return nil, lerrors.New(lerrors.ErrCorruptedFile).
			Op("read_header_async").
			Context("message", "read header failed").
			Wrap(err).
			Build()
	}

	if err := reader.readFooter(); err != nil {
		asyncIO.ReleaseFile(fileID)
		return nil, lerrors.New(lerrors.ErrCorruptedFile).
			Op("read_footer_async").
			Context("message", "read footer failed").
			Wrap(err).
			Build()
	}

	return reader, nil
}

// generateFileID 生成唯一的文件 ID
// 格式: filename_timestamp_counter
var fileIDCounter atomic.Uint64

func generateFileID(filename string) string {
	// 使用绝对路径确保唯一性
	absPath, err := filepath.Abs(filename)
	if err != nil {
		absPath = filename
	}

	id := fileIDCounter.Add(1)
	// 更健壮：hash(absPath) + counter
	hash := fnv.New64a()
	hash.Write([]byte(absPath))
	return fmt.Sprintf("file_%x_%d", hash.Sum64(), id)
}

// readHeader reads the file header
func (r *Reader) readHeader() error {
	if _, err := r.file.Seek(0, io.SeekStart); err != nil {
		return err
	}

	r.header = &format.Header{}
	if _, err := r.header.ReadFrom(r.file); err != nil {
		return err
	}

	return nil
}

// readFooter reads the file footer
func (r *Reader) readFooter() error {
	fileInfo, err := r.file.Stat()
	if err != nil {
		return err
	}

	footerOffset := fileInfo.Size() - format.FooterSize
	if _, err := r.file.Seek(footerOffset, io.SeekStart); err != nil {
		return err
	}

	r.footer = &format.Footer{}
	if _, err := r.footer.ReadFrom(r.file); err != nil {
		return err
	}

	return nil
}

// Schema returns the schema of the file
func (r *Reader) Schema() *arrow.Schema {
	return r.header.Schema
}

// NumRows returns the total number of rows in the file
func (r *Reader) NumRows() int64 {
	return r.header.NumRows
}

// ReadRecordBatch reads all data and returns a RecordBatch
// 根据 Reader 配置自动选择同步或异步模式
func (r *Reader) ReadRecordBatch() (*arrow.RecordBatch, error) {
	if r.closed {
		return nil, lerrors.New(lerrors.ErrInvalidArgument).
			Op("read_record_batch").
			Context("message", "reader is closed").
			Build()
	}

	schema := r.header.Schema
	numColumns := schema.NumFields()

	columns := make([]arrow.Array, numColumns)
	var readErr error

	if r.useAsync && r.asyncEnabled {
		// 异步模式：并发读取所有列
		readErr = r.readColumnsAsync(columns)
	} else {
		// 同步模式：顺序读取
		readErr = r.readColumnsSync(columns)
	}

	if readErr != nil {
		return nil, readErr
	}

	batch, err := arrow.NewRecordBatch(schema, int(r.header.NumRows), columns)
	if err != nil {
		return nil, lerrors.New(lerrors.ErrInvalidArgument).
			Op("create_record_batch").
			Context("message", "create record batch failed").
			Wrap(err).
			Build()
	}

	return batch, nil
}

// readColumnsSync 同步读取所有列
func (r *Reader) readColumnsSync(columns []arrow.Array) error {
	schema := r.header.Schema
	for colIdx := 0; colIdx < schema.NumFields(); colIdx++ {
		column, err := r.readColumn(int32(colIdx))
		if err != nil {
			lerrors.New(lerrors.ErrColumnNotFound).
				Op("read_columns_sync").
				Context("column_index", colIdx).
				Wrap(err).
				Build()
		}
		columns[colIdx] = column
	}
	return nil
}

// readColumnsAsync 异步并发读取所有列
func (r *Reader) readColumnsAsync(columns []arrow.Array) error {
	schema := r.header.Schema
	numColumns := schema.NumFields()

	// 使用 WaitGroup 等待所有列读取完成
	var wg sync.WaitGroup
	wg.Add(numColumns)

	errChan := make(chan error, numColumns)

	for colIdx := 0; colIdx < numColumns; colIdx++ {
		go func(idx int) {
			defer wg.Done()

			column, err := r.readColumnAsync(int32(idx))
			if err != nil {
				errChan <- lerrors.New(lerrors.ErrColumnNotFound).
					Op("read_columns_async").
					Context("column_index", idx).
					Wrap(err).
					Build()
				return
			}
			// 改进：添加边界检查
			if idx >= len(columns) {
				errChan <- lerrors.New(lerrors.ErrInvalidArgument).
					Op("read_columns_async").
					Context("column_index", idx).
					Context("message", "column index out of bounds").
					Build()
				return
			}
			columns[idx] = column
		}(colIdx)
	}

	wg.Wait()
	close(errChan)

	// 检查是否有错误
	for err := range errChan {
		if err != nil {
			return err
		}
	}

	return nil
}

// readColumn reads a single column from the file
func (r *Reader) readColumn(columnIndex int32) (arrow.Array, error) {
	pageIndices := r.footer.GetColumnPages(columnIndex)
	if len(pageIndices) == 0 {
		return nil, lerrors.PageNotFound("", columnIndex, 0)
	}

	if int(columnIndex) >= r.header.Schema.NumFields() {
		return nil, lerrors.New(lerrors.ErrInvalidArgument).
			Op("read_column").
			Context("column_index", columnIndex).
			Context("message", "column index out of range").
			Build()
	}
	field := r.header.Schema.Field(int(columnIndex))

	// 读取所有 pages
	var arrays []arrow.Array
	for _, pageIdx := range pageIndices {
		page, err := r.readPage(pageIdx)
		if err != nil {
			return nil, lerrors.IO("read_page", "", err)
		}

		array, err := r.pageReader.ReadPage(page, field.Type)
		if err != nil {
			return nil, lerrors.New(lerrors.ErrDecodeFailed).
				Op("deserialize_page").
				Wrap(err).
				Build()
		}

		arrays = append(arrays, array)
	}

	if len(arrays) == 1 {
		return arrays[0], nil
	}

	return r.mergeArrays(arrays, field.Type)
}

// 批量异步读取所有 pages
func (r *Reader) readColumnAsync(columnIndex int32) (arrow.Array, error) {
	pageIndices := r.footer.GetColumnPages(columnIndex)
	if len(pageIndices) == 0 {
		return nil, fmt.Errorf("no pages found for column %d", columnIndex)
	}

	field := r.header.Schema.Field(int(columnIndex))

	// 使用已有的 readPagesAsync 批量读取
	arrays, err := r.readPagesAsync(pageIndices, field.Type)
	if err != nil {
		return nil, err
	}

	if len(arrays) == 1 {
		return arrays[0], nil
	}

	return r.mergeArrays(arrays, field.Type)
}

// readPageAsyncWithEncoding 使用指定编码异步读取 page
func (r *Reader) readPageAsyncWithEncoding(pageIdx format.PageIndex, encoding format.EncodingType, dataType arrow.DataType) (arrow.Array, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// 读取 page 原始数据
	resultCh := r.asyncIO.Read(ctx, r.fileID, pageIdx.Offset, pageIdx.Size)

	select {
	case result := <-resultCh:
		if result.Error != nil {
			return nil, result.Error
		}

		// 直接解码数据
		return r.pageReader.ReadPageFromData(result.Data, encoding, pageIdx.NumValues, dataType)

	case <-ctx.Done():
		return nil, lerrors.New(lerrors.ErrTimeout).
			Op("read_page_async_with_encoding").
			Context("message", "timeout reading page").
			Build()
	}
}

// readPagesAsync 批量异步读取多个 Page
// 【修改】修复 ReadPages 使用方式，避免超时
// readPagesAsync 批量异步读取多个 Page
// 【修改】使用 SubmitBatch 批量提交，避免过多 goroutine
func (r *Reader) readPagesAsync(pageIndices []format.PageIndex, dataType arrow.DataType) ([]arrow.Array, error) {
	if !r.useAsync || !r.asyncEnabled {
		return r.readPagesSync(pageIndices, dataType)
	}

	if len(pageIndices) == 0 {
		return []arrow.Array{}, nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	arrays := make([]arrow.Array, len(pageIndices))
	errChan := make(chan error, len(pageIndices))
	var wg sync.WaitGroup

	// 【修改】限制并发度，避免过多 goroutine
	const maxConcurrency = 8
	semaphore := make(chan struct{}, maxConcurrency)

	for i, pIdx := range pageIndices {
		wg.Add(1)
		semaphore <- struct{}{} // 获取信号量

		go func(idx int, pageIdx format.PageIndex) {
			defer wg.Done()
			defer func() { <-semaphore }() // 释放信号量

			// 【修改】使用单个 Read，但共享同一个 AsyncIO 调度器
			resultCh := r.asyncIO.Read(ctx, r.fileID, pageIdx.Offset, pageIdx.Size)

			select {
			case result := <-resultCh:
				if result.Error != nil {
					errChan <- lerrors.New(lerrors.ErrIO).
						Op("read_pages_async").
						Context("page_index", idx).
						Wrap(result.Error).
						Build()
					return
				}

				array, err := r.pageReader.ReadPageFromData(
					result.Data,
					pageIdx.Encoding,
					pageIdx.NumValues,
					dataType,
				)
				if err != nil {
					errChan <- lerrors.New(lerrors.ErrDecodeFailed).
						Op("decode_page_async").
						Context("page_index", idx).
						Wrap(err).
						Build()
					return
				}

				arrays[idx] = array

			case <-ctx.Done():
				errChan <- lerrors.New(lerrors.ErrTimeout).
					Op("read_pages_async").
					Context("page_index", idx).
					Context("message", "timeout reading page").
					Build()
				return
			}
		}(i, pIdx)
	}

	wg.Wait()
	close(errChan)

	for err := range errChan {
		if err != nil {
			return nil, err
		}
	}

	return arrays, nil
}

// readPagesSync 同步读取多个 Page（回退方案）
func (r *Reader) readPagesSync(pageIndices []format.PageIndex, dataType arrow.DataType) ([]arrow.Array, error) {
	arrays := make([]arrow.Array, len(pageIndices))

	for i, pageIdx := range pageIndices {
		page, err := r.readPage(pageIdx)
		if err != nil {
			return nil, lerrors.New(lerrors.ErrIO).
				Op("read_pages_sync").
				Context("page_index", i).
				Wrap(err).
				Build()
		}

		array, err := r.pageReader.ReadPage(page, dataType)
		if err != nil {
			return nil, lerrors.New(lerrors.ErrDecodeFailed).
				Op("deserialize_page_sync").
				Context("page_index", i).
				Wrap(err).
				Build()
		}

		arrays[i] = array
	}

	return arrays, nil
}

// readPage reads a single page from the file
// 优先使用 AsyncIO（如果启用），否则使用同步 I/O
func (r *Reader) readPage(pageIndex format.PageIndex) (*format.Page, error) {
	// 如果 AsyncIO 启用，使用异步读取
	if r.useAsync && r.asyncEnabled {
		return r.readPageAsync(pageIndex)
	}

	// 同步读取
	return r.readPageSync(pageIndex)
}

// readPageSync 同步读取 Page
func (r *Reader) readPageSync(pageIndex format.PageIndex) (*format.Page, error) {
	if _, err := r.file.Seek(pageIndex.Offset, io.SeekStart); err != nil {
		return nil, err
	}

	page := &format.Page{}
	if _, err := page.ReadFrom(r.file); err != nil {
		return nil, err
	}

	return page, nil
}

// readPageAsync 异步读取 Page
func (r *Reader) readPageAsync(pageIndex format.PageIndex) (*format.Page, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// 使用 AsyncIO 读取
	resultCh := r.asyncIO.Read(ctx, r.fileID, pageIndex.Offset, pageIndex.Size)

	select {
	case result := <-resultCh:
		if result.Error != nil {
			return nil, lerrors.New(lerrors.ErrIO).
				Op("read_page_async").
				Wrap(result.Error).
				Build()
		}

		// 从 result.Data 构造 Page
		page := &format.Page{}
		if err := page.UnmarshalBinary(result.Data); err != nil {
			return nil, lerrors.New(lerrors.ErrCorruptedFile).
				Op("unmarshal_page").
				Wrap(err).
				Build()
		}

		return page, nil

	case <-ctx.Done():
		return nil, lerrors.New(lerrors.ErrTimeout).
			Op("read_page_async").
			Context("message", "async read timeout").
			Build()
	}
}

// mergeArrays merges multiple arrays of the same type into one
func (r *Reader) mergeArrays(arrays []arrow.Array, dataType arrow.DataType) (arrow.Array, error) {
	if len(arrays) == 0 {
		return nil, lerrors.New(lerrors.ErrInvalidArgument).
			Op("merge_arrays").
			Context("message", "no arrays to merge").
			Build()
	}

	if len(arrays) == 1 {
		return arrays[0], nil
	}

	switch dataType.ID() {
	case arrow.INT32:
		return r.mergeInt32Arrays(arrays)
	case arrow.INT64:
		return r.mergeInt64Arrays(arrays)
	case arrow.FLOAT32:
		return r.mergeFloat32Arrays(arrays)
	case arrow.FLOAT64:
		return r.mergeFloat64Arrays(arrays)
	case arrow.FIXED_SIZE_LIST:
		return r.mergeFixedSizeListArrays(arrays, dataType.(*arrow.FixedSizeListType))
	default:
		return nil, lerrors.UnsupportedType("merge_arrays", dataType.Name(), "")
	}
}

// mergeInt32Arrays merges multiple Int32Array into one
func (r *Reader) mergeInt32Arrays(arrays []arrow.Array) (arrow.Array, error) {
	builder := arrow.NewInt32Builder()
	defer builder.Release()

	// Calculate total size for reservation
	totalSize := 0
	for _, arr := range arrays {
		totalSize += arr.Len()
	}
	builder.Reserve(totalSize)

	// Append all values
	for _, arr := range arrays {
		int32Arr := arr.(*arrow.Int32Array)
		for i := 0; i < int32Arr.Len(); i++ {
			if int32Arr.IsNull(i) {
				builder.AppendNull()
			} else {
				builder.Append(int32Arr.Value(i))
			}
		}
	}

	return builder.NewArray(), nil
}

// mergeInt64Arrays merges multiple Int64Array into one
func (r *Reader) mergeInt64Arrays(arrays []arrow.Array) (arrow.Array, error) {
	builder := &arrow.Int64Builder{}

	totalSize := 0
	for _, arr := range arrays {
		totalSize += arr.Len()
	}
	builder.Reserve(totalSize)

	for _, arr := range arrays {
		int64Arr := arr.(*arrow.Int64Array)
		for i := 0; i < int64Arr.Len(); i++ {
			if int64Arr.IsNull(i) {
				builder.AppendNull()
			} else {
				builder.Append(int64Arr.Value(i))
			}
		}
	}

	return builder.NewArray(), nil
}

// mergeFloat32Arrays merges multiple Float32Array into one
func (r *Reader) mergeFloat32Arrays(arrays []arrow.Array) (arrow.Array, error) {
	builder := arrow.NewFloat32Builder()
	defer builder.Release()

	totalSize := 0
	for _, arr := range arrays {
		totalSize += arr.Len()
	}
	builder.Reserve(totalSize)

	for _, arr := range arrays {
		float32Arr := arr.(*arrow.Float32Array)
		for i := 0; i < float32Arr.Len(); i++ {
			if float32Arr.IsNull(i) {
				builder.AppendNull()
			} else {
				builder.Append(float32Arr.Value(i))
			}
		}
	}

	return builder.NewArray(), nil
}

// mergeFloat64Arrays merges multiple Float64Array into one
func (r *Reader) mergeFloat64Arrays(arrays []arrow.Array) (arrow.Array, error) {
	builder := &arrow.Float64Builder{}

	totalSize := 0
	for _, arr := range arrays {
		totalSize += arr.Len()
	}
	builder.Reserve(totalSize)

	for _, arr := range arrays {
		float64Arr := arr.(*arrow.Float64Array)
		for i := 0; i < float64Arr.Len(); i++ {
			if float64Arr.IsNull(i) {
				builder.AppendNull()
			} else {
				builder.Append(float64Arr.Value(i))
			}
		}
	}

	return builder.NewArray(), nil
}

// mergeFixedSizeListArrays merges multiple FixedSizeListArray into one
func (r *Reader) mergeFixedSizeListArrays(arrays []arrow.Array, listType *arrow.FixedSizeListType) (arrow.Array, error) {
	builder := arrow.NewFixedSizeListBuilder(listType)
	defer builder.Release()

	totalSize := 0
	for _, arr := range arrays {
		totalSize += arr.Len()
	}
	builder.Reserve(totalSize)

	for _, arr := range arrays {
		listArr := arr.(*arrow.FixedSizeListArray)

		for i := 0; i < listArr.Len(); i++ {
			if listArr.IsNull(i) {
				builder.AppendNull()
			} else {
				// Get values for this list
				values := r.getFixedSizeListValues(listArr, i)
				builder.AppendValues(values)
			}
		}
	}

	return builder.NewArray(), nil
}

// getFixedSizeListValues extracts values from a FixedSizeListArray at index i
func (r *Reader) getFixedSizeListValues(arr *arrow.FixedSizeListArray, index int) []float32 {
	listSize := arr.ListSize()
	values := make([]float32, listSize)

	// Get the underlying values array
	valuesArray := arr.Values()

	// Calculate offset in values array
	startOffset := index * listSize

	switch valArr := valuesArray.(type) {
	case *arrow.Float32Array:
		for j := 0; j < listSize; j++ {
			values[j] = valArr.Value(startOffset + j)
		}
	case *arrow.Int32Array:
		for j := 0; j < listSize; j++ {
			values[j] = float32(valArr.Value(startOffset + j))
		}
	}

	return values
}

// Close 方法
func (r *Reader) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.closed {
		return lerrors.New(lerrors.ErrInvalidArgument).
			Op("close_reader").
			Context("message", "reader already closed").
			Build()
	}
	r.closed = true

	if r.useAsync && r.asyncIO != nil {
		// 异步模式：释放 FilePool 引用
		// FilePool 负责真正关闭文件
		return r.asyncIO.ReleaseFile(r.fileID)
	}

	// 同步模式：自己关闭文件
	if r.file != nil {
		return r.file.Close()
	}
	return nil
}
