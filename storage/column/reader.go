package column

import (
	"context"
	"fmt"
	"hash/fnv"
	"io"
	"sort"
	"github.com/wzqhbustb/vego/core"
	"github.com/wzqhbustb/vego/storage/format"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/wzqhbustb/vego/vfs"
)

// Reader reads RecordBatch data from a Lance file
type Reader struct {
	file       vfs.File
	header     *format.Header
	footer     *format.Footer
	pageReader *PageReader
	closed     bool
	mu         sync.Mutex
	stats      *format.StatisticsList // Column statistics for Zone Map optimization

	// Phase 2: 异步 I/O 支持（可选）
	asyncIO      *vfs.AsyncIO
	fileID       string // 在 AsyncIO 中注册的文件 ID
	useAsync     bool   // 是否启用异步模式
	asyncEnabled bool   // AsyncIO 是否可用（文件已注册）

	// BlockCache 支持（可选）
	blockCache *format.BlockCache // 页面缓存实例
	cacheKey   string             // 文件唯一标识（用于缓存键）

	// Range coalescing config (Wave 2)
	coalesceGap  int64 // max gap between pages to merge (default 4KB)
	maxMergeSize int64 // max size of a merged range (default 1MB)

	// Metadata caching (Wave 3)
	footerOnce sync.Once // ensures footer is loaded only once per Reader instance
	footerErr  error     // cached error from footer load
}

// NewReader creates a new column reader（同步模式）using the default local VFS.
func NewReader(filename string) (*Reader, error) {
	return NewReaderWithVFS(filename, vfs.Local)
}

// NewReaderWithVFS creates a new column reader with a custom VFS.
func NewReaderWithVFS(filename string, fs vfs.VFS) (*Reader, error) {
	file, err := fs.Open(filename)
	if err != nil {
		return nil, core.IO("new_reader", filename, err)
	}

	reader := &Reader{
		file:         file,
		pageReader:   NewPageReader(),
		closed:       false,
		useAsync:     false, // 默认同步模式
		coalesceGap:  4 * 1024,    // 4KB default
		maxMergeSize: 1024 * 1024, // 1MB default
	}

	// Read header
	if err := reader.readHeader(); err != nil {
		file.Close()
		return nil, core.New(core.ErrCorruptedFile).
			Op("read_header").
			Context("message", "read header failed").
			Wrap(err).
			Build()
	}

	// Read footer (cached via sync.Once)
	if err := reader.loadFooter(); err != nil {
		file.Close()
		return nil, core.New(core.ErrCorruptedFile).
			Op("read_footer").
			Context("message", "read footer failed").
			Wrap(err).
			Build()
	}

	return reader, nil
}

// NewReaderWithCache creates a new column reader with BlockCache support using the default local VFS.
// The cache parameter can be shared across multiple readers for the same or different files.
func NewReaderWithCache(filename string, cache *format.BlockCache) (*Reader, error) {
	return NewReaderWithCacheAndVFS(filename, vfs.Local, cache)
}

// NewReaderWithCacheAndVFS creates a new column reader with BlockCache support using a custom VFS.
func NewReaderWithCacheAndVFS(filename string, fs vfs.VFS, cache *format.BlockCache) (*Reader, error) {
	reader, err := NewReaderWithVFS(filename, fs)
	if err != nil {
		return nil, err
	}

	if cache != nil {
		reader.blockCache = cache
		reader.cacheKey = GenerateCacheKey(filename)
	}

	return reader, nil
}

// GenerateCacheKey generates a unique cache key for a file
// Uses absolute path hash to ensure uniqueness
// This is exported for cache invalidation purposes
func GenerateCacheKey(filename string) string {
	absPath, err := filepath.Abs(filename)
	if err != nil {
		absPath = filename
	}

	hash := fnv.New64a()
	hash.Write([]byte(absPath))

	return fmt.Sprintf("lance:%x", hash.Sum64())
}

// NewReaderWithAsyncIO 不需要自己打开文件
func NewReaderWithAsyncIO(filename string, asyncIO *vfs.AsyncIO) (*Reader, error) {
	if asyncIO == nil {
		return NewReader(filename)
	}

	fileID := generateFileID(filename)

	// 1. 注册文件到 AsyncIO/FilePool
	if err := asyncIO.RegisterFile(fileID, filename); err != nil {
		return nil, core.New(core.ErrIO).
			Op("register_file_async").
			Context("file_id", fileID).
			Wrap(err).
			Build()
	}

	// 2. 获取文件句柄（增加引用计数）
	file, err := asyncIO.GetFile(fileID)
	if err != nil {
		return nil, core.New(core.ErrIO).
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
		coalesceGap:  4 * 1024,    // 4KB default
		maxMergeSize: 1024 * 1024, // 1MB default
	}

	// 读取 header/footer（使用 FilePool 的句柄）
	if err := reader.readHeader(); err != nil {
		asyncIO.ReleaseFile(fileID) // 清理
		return nil, core.New(core.ErrCorruptedFile).
			Op("read_header_async").
			Context("message", "read header failed").
			Wrap(err).
			Build()
	}

	if err := reader.loadFooter(); err != nil {
		asyncIO.ReleaseFile(fileID)
		return nil, core.New(core.ErrCorruptedFile).
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

// loadFooter loads footer and column statistics using sync.Once.
// Thread-safe: multiple concurrent calls only trigger one I/O.
func (r *Reader) loadFooter() error {
	r.footerOnce.Do(func() {
		r.footerErr = r.readFooterFromFile()
	})
	return r.footerErr
}

// readFooterFromFile performs the actual footer I/O.
func (r *Reader) readFooterFromFile() error {
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

	// Read column statistics if available
	if r.footer.StatsOffset > 0 && r.footer.StatsCount > 0 {
		if _, err := r.file.Seek(r.footer.StatsOffset, io.SeekStart); err != nil {
			return core.IO("seek_stats", "", err)
		}
		r.stats = &format.StatisticsList{}
		if _, err := r.stats.ReadFrom(r.file); err != nil {
			return core.IO("read_stats", "", err)
		}
	}

	return nil
}

// Schema returns the schema of the file
func (r *Reader) Schema() *core.Schema {
	return r.header.Schema
}

// NumRows returns the total number of rows in the file
func (r *Reader) NumRows() int64 {
	return r.header.NumRows
}

// ReadRecordBatch reads all data and returns a RecordBatch
// 根据 Reader 配置自动选择同步或异步模式
func (r *Reader) ReadRecordBatch() (*core.RecordBatch, error) {
	if r.closed {
		return nil, core.New(core.ErrInvalidArgument).
			Op("read_record_batch").
			Context("message", "reader is closed").
			Build()
	}

	schema := r.header.Schema
	numColumns := schema.NumFields()

	columns := make([]core.Array, numColumns)
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

	batch, err := core.NewRecordBatch(schema, int(r.header.NumRows), columns)
	if err != nil {
		return nil, core.New(core.ErrInvalidArgument).
			Op("create_record_batch").
			Context("message", "create record batch failed").
			Wrap(err).
			Build()
	}

	return batch, nil
}

// readColumnsSync 同步读取所有列
func (r *Reader) readColumnsSync(columns []core.Array) error {
	schema := r.header.Schema
	for colIdx := 0; colIdx < schema.NumFields(); colIdx++ {
		column, err := r.readColumn(int32(colIdx))
		if err != nil {
			return core.New(core.ErrColumnNotFound).
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
func (r *Reader) readColumnsAsync(columns []core.Array) error {
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
				errChan <- core.New(core.ErrColumnNotFound).
					Op("read_columns_async").
					Context("column_index", idx).
					Wrap(err).
					Build()
				return
			}
			// 改进：添加边界检查
			if idx >= len(columns) {
				errChan <- core.New(core.ErrInvalidArgument).
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
func (r *Reader) readColumn(columnIndex int32) (core.Array, error) {
	pageIndices := r.footer.GetColumnPages(columnIndex)
	if len(pageIndices) == 0 {
		return nil, core.PageNotFound("", columnIndex, 0)
	}

	if int(columnIndex) >= r.header.Schema.NumFields() {
		return nil, core.New(core.ErrInvalidArgument).
			Op("read_column").
			Context("column_index", columnIndex).
			Context("message", "column index out of range").
			Build()
	}
	field := r.header.Schema.Field(int(columnIndex))

	// 读取所有 pages
	var arrays []core.Array
	for _, pageIdx := range pageIndices {
		page, err := r.readPage(pageIdx)
		if err != nil {
			return nil, core.IO("read_page", "", err)
		}

		array, err := r.pageReader.ReadPage(page, field.Type)
		if err != nil {
			return nil, core.New(core.ErrDecodeFailed).
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
func (r *Reader) readColumnAsync(columnIndex int32) (core.Array, error) {
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

// mergedRange represents a coalesced read range covering multiple pages.
type mergedRange struct {
	offset  int64
	size    int64
	indices []int // original positions in pageIndices slice
}

// coalesceRanges merges adjacent page read requests into larger ranges.
// Pages are sorted by offset; two pages merge if gap <= maxGap and merged size <= maxMergeSize.
func coalesceRanges(pages []format.PageIndex, maxGap int64, maxMergeSize int64) []mergedRange {
	if len(pages) == 0 {
		return nil
	}

	const defaultMaxMergeSize = 1024 * 1024 // 1MB, prevents excessive memory spike
	if maxMergeSize <= 0 {
		maxMergeSize = defaultMaxMergeSize
	}

	// Sort by offset, keeping track of original indices
	type indexed struct {
		idx int
		p   format.PageIndex
	}
	ordered := make([]indexed, len(pages))
	for i, p := range pages {
		ordered[i] = indexed{i, p}
	}
	sort.SliceStable(ordered, func(i, j int) bool {
		return ordered[i].p.Offset < ordered[j].p.Offset
	})

	var ranges []mergedRange
	cur := mergedRange{
		offset:  ordered[0].p.Offset,
		size:    int64(ordered[0].p.Size),
		indices: []int{ordered[0].idx},
	}

	for i := 1; i < len(ordered); i++ {
		p := ordered[i].p
		end := cur.offset + cur.size
		gap := p.Offset - end
		candidateSize := p.Offset + int64(p.Size) - cur.offset

		if gap <= maxGap && candidateSize <= maxMergeSize {
			cur.size = candidateSize
			cur.indices = append(cur.indices, ordered[i].idx)
		} else {
			ranges = append(ranges, cur)
			cur = mergedRange{
				offset:  p.Offset,
				size:    int64(p.Size),
				indices: []int{ordered[i].idx},
			}
		}
	}
	ranges = append(ranges, cur)
	return ranges
}

// readPagesAsync reads multiple pages using range coalescing:
// adjacent page requests are merged into fewer, larger ReadAt calls.
//
// Design note: This intentionally calls r.file.ReadAt directly instead of
// r.asyncIO.Read(). Range coalescing has already reduced N page requests to
// M ranges (typically M << N), so goroutine-per-range with synchronous ReadAt
// is simple and efficient. Re-splitting into individual IORequests would defeat
// the coalescing benefit. Future backpressure (Wave 6) can submit merged ranges
// as large IORequests if needed.
func (r *Reader) readPagesAsync(pageIndices []format.PageIndex, dataType core.DataType) ([]core.Array, error) {
	if !r.useAsync || !r.asyncEnabled {
		return r.readPagesSync(pageIndices, dataType)
	}

	if len(pageIndices) == 0 {
		return []core.Array{}, nil
	}

	// 1. Coalesce adjacent ranges
	ranges := coalesceRanges(pageIndices, r.coalesceGap, r.maxMergeSize)

	// 2. Read each merged range directly via ReadAt
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	arrays := make([]core.Array, len(pageIndices))
	errChan := make(chan error, len(ranges))
	var wg sync.WaitGroup

	// Limit concurrency even after coalescing: if coalesce is ineffective
	// (all pages spaced > maxGap), goroutine count equals page count.
	const maxConcurrency = 8
	semaphore := make(chan struct{}, maxConcurrency)

	for _, mr := range ranges {
		wg.Add(1)
		semaphore <- struct{}{} // acquire

		go func(mr mergedRange) {
			defer wg.Done()
			defer func() { <-semaphore }() // release

			select {
			case <-ctx.Done():
				errChan <- core.New(core.ErrTimeout).
					Op("read_pages_async").
					Context("offset", mr.offset).
					Context("message", "timeout before read").
					Build()
				return
			default:
			}

			buf := make([]byte, mr.size)
			n, err := r.file.ReadAt(buf, mr.offset)
			if err != nil {
				errChan <- core.New(core.ErrIO).
					Op("read_pages_async").
					Context("offset", mr.offset).
					Context("size", mr.size).
					Wrap(err).
					Build()
				return
			}
			if int64(n) < mr.size {
				errChan <- core.New(core.ErrIO).
					Op("read_pages_async").
					Context("offset", mr.offset).
					Context("expected", mr.size).
					Context("got", n).
					Context("message", "short read").
					Build()
				return
			}

			// 3. Slice by original boundaries and decode
			for _, pageIdx := range mr.indices {
				p := pageIndices[pageIdx]
				pageOff := p.Offset - mr.offset
				pageBuf := buf[pageOff : pageOff+int64(p.Size)]
				array, err := r.pageReader.ReadPageFromData(
					pageBuf,
					p.Encoding,
					p.NumValues,
					dataType,
				)
				if err != nil {
					errChan <- core.New(core.ErrDecodeFailed).
						Op("decode_page_async").
						Context("page_index", pageIdx).
						Wrap(err).
						Build()
					return
				}
				arrays[pageIdx] = array
			}
		}(mr)
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
func (r *Reader) readPagesSync(pageIndices []format.PageIndex, dataType core.DataType) ([]core.Array, error) {
	arrays := make([]core.Array, len(pageIndices))

	for i, pageIdx := range pageIndices {
		page, err := r.readPage(pageIdx)
		if err != nil {
			return nil, core.New(core.ErrIO).
				Op("read_pages_sync").
				Context("page_index", i).
				Wrap(err).
				Build()
		}

		array, err := r.pageReader.ReadPage(page, dataType)
		if err != nil {
			return nil, core.New(core.ErrDecodeFailed).
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
// 如果 BlockCache 启用，优先从缓存读取
func (r *Reader) readPage(pageIndex format.PageIndex) (*format.Page, error) {
	// 1. 尝试从 BlockCache 读取
	if r.blockCache != nil {
		page, hit := r.readPageFromCache(pageIndex)
		if hit {
			return page, nil
		}
	}

	// 2. 缓存未命中，根据配置选择读取方式
	var page *format.Page
	var err error

	if r.useAsync && r.asyncEnabled {
		page, err = r.readPageAsync(pageIndex)
	} else {
		page, err = r.readPageSync(pageIndex)
	}

	if err != nil {
		return nil, err
	}

	// 3. 写入缓存
	if r.blockCache != nil {
		r.writePageToCache(pageIndex, page)
	}

	return page, nil
}

// readPageFromCache 尝试从 BlockCache 读取 Page
func (r *Reader) readPageFromCache(pageIndex format.PageIndex) (*format.Page, bool) {
	cacheKey := r.generatePageCacheKey(pageIndex)

	data, found := r.blockCache.Get(cacheKey)
	if !found {
		return nil, false
	}

	// 反序列化
	page := &format.Page{}
	if err := page.UnmarshalBinary(data); err != nil {
		// 缓存数据损坏，移除该条目
		r.blockCache.Remove(cacheKey)
		return nil, false
	}

	return page, true
}

// writePageToCache 将 Page 写入 BlockCache
func (r *Reader) writePageToCache(pageIndex format.PageIndex, page *format.Page) {
	cacheKey := r.generatePageCacheKey(pageIndex)

	data, err := page.MarshalBinary()
	if err != nil {
		return // 序列化失败，跳过缓存
	}

	r.blockCache.Put(cacheKey, data)
}

// generatePageCacheKey 生成 Page 的缓存键
// 格式: {cacheKey}:page:{offset}:{size}
func (r *Reader) generatePageCacheKey(pageIndex format.PageIndex) string {
	return fmt.Sprintf("%s:page:%d:%d", r.cacheKey, pageIndex.Offset, pageIndex.Size)
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
			return nil, core.New(core.ErrIO).
				Op("read_page_async").
				Wrap(result.Error).
				Build()
		}

		// 从 result.Data 构造 Page
		page := &format.Page{}
		if err := page.UnmarshalBinary(result.Data); err != nil {
			return nil, core.New(core.ErrCorruptedFile).
				Op("unmarshal_page").
				Wrap(err).
				Build()
		}

		return page, nil

	case <-ctx.Done():
		return nil, core.New(core.ErrTimeout).
			Op("read_page_async").
			Context("message", "async read timeout").
			Build()
	}
}

// mergeArrays merges multiple arrays of the same type into one
func (r *Reader) mergeArrays(arrays []core.Array, dataType core.DataType) (core.Array, error) {
	if len(arrays) == 0 {
		return nil, core.New(core.ErrInvalidArgument).
			Op("merge_arrays").
			Context("message", "no arrays to merge").
			Build()
	}

	if len(arrays) == 1 {
		return arrays[0], nil
	}

	switch dataType.ID() {
	case core.INT32:
		return r.mergeInt32Arrays(arrays)
	case core.INT64:
		return r.mergeInt64Arrays(arrays)
	case core.FLOAT32:
		return r.mergeFloat32Arrays(arrays)
	case core.FLOAT64:
		return r.mergeFloat64Arrays(arrays)
	case core.FIXED_SIZE_LIST:
		return r.mergeFixedSizeListArrays(arrays, dataType.(*core.FixedSizeListType))
	default:
		return nil, core.UnsupportedType("merge_arrays", dataType.Name(), "")
	}
}

// mergeInt32Arrays merges multiple Int32Array into one
func (r *Reader) mergeInt32Arrays(arrays []core.Array) (core.Array, error) {
	builder := core.NewInt32Builder()
	defer builder.Release()

	// Calculate total size for reservation
	totalSize := 0
	for _, arr := range arrays {
		totalSize += arr.Len()
	}
	builder.Reserve(totalSize)

	// Append all values
	for _, arr := range arrays {
		int32Arr := arr.(*core.Int32Array)
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
func (r *Reader) mergeInt64Arrays(arrays []core.Array) (core.Array, error) {
	builder := &core.Int64Builder{}

	totalSize := 0
	for _, arr := range arrays {
		totalSize += arr.Len()
	}
	builder.Reserve(totalSize)

	for _, arr := range arrays {
		int64Arr := arr.(*core.Int64Array)
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
func (r *Reader) mergeFloat32Arrays(arrays []core.Array) (core.Array, error) {
	builder := core.NewFloat32Builder()
	defer builder.Release()

	totalSize := 0
	for _, arr := range arrays {
		totalSize += arr.Len()
	}
	builder.Reserve(totalSize)

	for _, arr := range arrays {
		float32Arr := arr.(*core.Float32Array)
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
func (r *Reader) mergeFloat64Arrays(arrays []core.Array) (core.Array, error) {
	builder := &core.Float64Builder{}

	totalSize := 0
	for _, arr := range arrays {
		totalSize += arr.Len()
	}
	builder.Reserve(totalSize)

	for _, arr := range arrays {
		float64Arr := arr.(*core.Float64Array)
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
func (r *Reader) mergeFixedSizeListArrays(arrays []core.Array, listType *core.FixedSizeListType) (core.Array, error) {
	builder := core.NewFixedSizeListBuilder(listType)
	defer builder.Release()

	totalSize := 0
	for _, arr := range arrays {
		totalSize += arr.Len()
	}
	builder.Reserve(totalSize)

	for _, arr := range arrays {
		listArr := arr.(*core.FixedSizeListArray)

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
func (r *Reader) getFixedSizeListValues(arr *core.FixedSizeListArray, index int) []float32 {
	listSize := arr.ListSize()
	values := make([]float32, listSize)

	// Get the underlying values array
	valuesArray := arr.Values()

	// Calculate offset in values array
	startOffset := index * listSize

	switch valArr := valuesArray.(type) {
	case *core.Float32Array:
		for j := 0; j < listSize; j++ {
			values[j] = valArr.Value(startOffset + j)
		}
	case *core.Int32Array:
		for j := 0; j < listSize; j++ {
			values[j] = float32(valArr.Value(startOffset + j))
		}
	}

	return values
}

// ReadRowAt reads a single row at the specified index across all columns.
// This enables O(1) random access when combined with RowIndex.
func (r *Reader) ReadRowAt(rowIdx int64) ([]interface{}, error) {
	if r.closed {
		return nil, core.New(core.ErrInvalidArgument).
			Op("read_row_at").
			Context("message", "reader is closed").
			Build()
	}

	if rowIdx < 0 || rowIdx >= r.header.NumRows {
		return nil, core.New(core.ErrInvalidArgument).
			Op("read_row_at").
			Context("row_idx", rowIdx).
			Context("num_rows", r.header.NumRows).
			Context("message", "row index out of range").
			Build()
	}

	schema := r.header.Schema
	numColumns := schema.NumFields()
	
	// Read the specific row from each column
	rowValues := make([]interface{}, numColumns)
	
	for colIdx := 0; colIdx < numColumns; colIdx++ {
		value, err := r.readColumnRowAt(int32(colIdx), rowIdx)
		if err != nil {
			return nil, core.New(core.ErrIO).
				Op("read_row_at_column").
				Context("column", colIdx).
				Context("row", rowIdx).
				Wrap(err).
				Build()
		}
		rowValues[colIdx] = value
	}
	
	return rowValues, nil
}

// readColumnRowAt reads a single value from the specified column and row.
func (r *Reader) readColumnRowAt(columnIndex int32, rowIdx int64) (interface{}, error) {
	pageIndices := r.footer.GetColumnPages(columnIndex)
	if len(pageIndices) == 0 {
		return nil, core.PageNotFound("", columnIndex, 0)
	}

	// Find which page contains the row
	var targetPage format.PageIndex
	var pageStartRow int64 = 0
	found := false
	
	for _, pageIdx := range pageIndices {
		if rowIdx < pageStartRow+int64(pageIdx.NumValues) {
			targetPage = pageIdx
			found = true
			break
		}
		pageStartRow += int64(pageIdx.NumValues)
	}
	
	if !found {
		return nil, core.New(core.ErrInvalidArgument).
			Op("read_column_row_at").
			Context("column", columnIndex).
			Context("row", rowIdx).
			Context("message", "row not found in any page").
			Build()
	}

	// Read the page
	page, err := r.readPage(targetPage)
	if err != nil {
		return nil, err
	}

	// Get the field type
	field := r.header.Schema.Field(int(columnIndex))
	
	// Read the page into an array
	array, err := r.pageReader.ReadPage(page, field.Type)
	if err != nil {
		return nil, err
	}
	
	// Extract the specific row value from the page
	localRowIdx := int(rowIdx - pageStartRow)
	if localRowIdx < 0 || localRowIdx >= array.Len() {
		return nil, core.New(core.ErrInvalidArgument).
			Op("read_column_row_at").
			Context("local_row_idx", localRowIdx).
			Context("array_len", array.Len()).
			Context("message", "local row index out of range").
			Build()
	}
	
	return r.extractValueFromArray(array, localRowIdx, field.Type)
}

// extractValueFromArray extracts a single value from an array at the given index.
// Returns nil if the value at idx is null.
func (r *Reader) extractValueFromArray(arr core.Array, idx int, dataType core.DataType) (interface{}, error) {
	// Check for null first
	if arr.IsNull(idx) {
		return nil, nil
	}

	switch arr := arr.(type) {
	case *core.Int64Array:
		return arr.Value(idx), nil
	case *core.Int32Array:
		return int64(arr.Value(idx)), nil
	case *core.Float32Array:
		return arr.Value(idx), nil
	case *core.Float64Array:
		return arr.Value(idx), nil
	case *core.FixedSizeListArray:
		// Handle vector type
		return r.getFixedSizeListValues(arr, idx), nil
	default:
		return nil, core.New(core.ErrInvalidArgument).
			Op("extract_value").
			Context("type", dataType.Name()).
			Context("message", "unsupported array type").
			Build()
	}
}

// Close 方法
func (r *Reader) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.closed {
		return core.New(core.ErrInvalidArgument).
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

// GetColumnStats returns the column statistics for a specific column.
// Returns nil if no statistics are available for the column.
func (r *Reader) GetColumnStats(columnIndex int32) *format.ColumnStatistics {
	if r.stats == nil {
		return nil
	}
	return r.stats.GetColumnStats(columnIndex)
}

// GetAllStats returns the complete statistics list for all columns.
// Returns nil if no statistics are available.
func (r *Reader) GetAllStats() *format.StatisticsList {
	return r.stats
}

// HasStatistics returns true if the file contains column statistics.
func (r *Reader) HasStatistics() bool {
	return r.stats != nil && r.stats.NumColumns > 0
}

// ZoneMapEvaluator returns a ZoneMapEvaluator for this reader's statistics.
// This can be used for predicate pushdown optimization.
func (r *Reader) ZoneMapEvaluator() *ZoneMapEvaluator {
	return NewZoneMapEvaluator(r.stats)
}

// EvaluateZoneMapInt32 checks if a column's min/max range overlaps with a predicate value.
// Deprecated: Use r.ZoneMapEvaluator().EvaluateZoneMapInt32() instead.
func (r *Reader) EvaluateZoneMapInt32(columnIndex int32, value int32) ZoneMapFilterResult {
	return r.ZoneMapEvaluator().EvaluateZoneMapInt32(columnIndex, value)
}

// EvaluateZoneMapInt64 checks if a column's min/max range overlaps with a predicate value.
// Deprecated: Use r.ZoneMapEvaluator().EvaluateZoneMapInt64() instead.
func (r *Reader) EvaluateZoneMapInt64(columnIndex int32, value int64) ZoneMapFilterResult {
	return r.ZoneMapEvaluator().EvaluateZoneMapInt64(columnIndex, value)
}

// EvaluateZoneMapFloat32 checks if a column's min/max range overlaps with a predicate value.
// Deprecated: Use r.ZoneMapEvaluator().EvaluateZoneMapFloat32() instead.
func (r *Reader) EvaluateZoneMapFloat32(columnIndex int32, value float32) ZoneMapFilterResult {
	return r.ZoneMapEvaluator().EvaluateZoneMapFloat32(columnIndex, value)
}

// EvaluateZoneMapFloat64 checks if a column's min/max range overlaps with a predicate value.
// Deprecated: Use r.ZoneMapEvaluator().EvaluateZoneMapFloat64() instead.
func (r *Reader) EvaluateZoneMapFloat64(columnIndex int32, value float64) ZoneMapFilterResult {
	return r.ZoneMapEvaluator().EvaluateZoneMapFloat64(columnIndex, value)
}

// EvaluateZoneMapRangeInt32 checks if a column's range overlaps with a query range [low, high].
// Deprecated: Use r.ZoneMapEvaluator().EvaluateZoneMapRangeInt32() instead.
func (r *Reader) EvaluateZoneMapRangeInt32(columnIndex int32, low, high int32) ZoneMapFilterResult {
	return r.ZoneMapEvaluator().EvaluateZoneMapRangeInt32(columnIndex, low, high)
}

// EvaluateZoneMapRangeInt64 checks if a column's range overlaps with a query range.
// Deprecated: Use r.ZoneMapEvaluator().EvaluateZoneMapRangeInt64() instead.
func (r *Reader) EvaluateZoneMapRangeInt64(columnIndex int32, low, high int64) ZoneMapFilterResult {
	return r.ZoneMapEvaluator().EvaluateZoneMapRangeInt64(columnIndex, low, high)
}

// EvaluateZoneMapRangeFloat32 checks if a column's range overlaps with a query range.
// Deprecated: Use r.ZoneMapEvaluator().EvaluateZoneMapRangeFloat32() instead.
func (r *Reader) EvaluateZoneMapRangeFloat32(columnIndex int32, low, high float32) ZoneMapFilterResult {
	return r.ZoneMapEvaluator().EvaluateZoneMapRangeFloat32(columnIndex, low, high)
}

// EvaluateZoneMapRangeFloat64 checks if a column's range overlaps with a query range.
// Deprecated: Use r.ZoneMapEvaluator().EvaluateZoneMapRangeFloat64() instead.
func (r *Reader) EvaluateZoneMapRangeFloat64(columnIndex int32, low, high float64) ZoneMapFilterResult {
	return r.ZoneMapEvaluator().EvaluateZoneMapRangeFloat64(columnIndex, low, high)
}
