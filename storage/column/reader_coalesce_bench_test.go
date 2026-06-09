package column

import (
	"path/filepath"
	"testing"

	"github.com/wzqhbustb/vego/core"
	"github.com/wzqhbustb/vego/vfs"
)

// createTestFileManyPages creates a file that produces many small pages,
// ideal for testing coalescing benefits.
func createTestFileManyPages(b testing.TB, filename string, numPagesPerCol int) {
	fields := []core.Field{
		{Name: "id", Type: core.PrimInt32(), Nullable: false},
	}
	schema := core.NewSchema(fields, nil)

	writer, err := NewWriter(filename, schema, defaultEncoderFactory())
	if err != nil {
		b.Fatalf("NewWriter failed: %v", err)
	}
	defer func() {
		if err := writer.Close(); err != nil {
			b.Errorf("writer.Close() failed: %v", err)
		}
	}()

	// Write many small batches to create multiple pages per column
	for p := 0; p < numPagesPerCol; p++ {
		builder := core.NewInt32Builder()
		for i := 0; i < 100; i++ {
			builder.Append(int32(p*1000 + i))
		}
		arr := builder.NewArray()
		builder.Release()

		batch, err := core.NewRecordBatch(schema, 100, []core.Array{arr})
		if err != nil {
			b.Fatalf("NewRecordBatch failed: %v", err)
		}
		if err := writer.WriteRecordBatch(batch); err != nil {
			b.Fatalf("WriteRecordBatch failed: %v", err)
		}
	}
}

// BenchmarkReadPagesCoalescing compares coalesced vs non-coalesced reads.
func BenchmarkReadPagesCoalescing(b *testing.B) {
	tmpDir := b.TempDir()
	filename := filepath.Join(tmpDir, "bench_coalesce.lance")

	numPages := 50
	createTestFileManyPages(b, filename, numPages)

	// Pre-create AsyncIO and register file
	config := vfs.DefaultConfig()
	config.Workers = 4
	asyncIO, err := vfs.New(config)
	if err != nil {
		b.Fatalf("Failed to create AsyncIO: %v", err)
	}
	defer asyncIO.Close()

	b.Run("no_coalesce", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			reader, err := NewReaderWithAsyncIO(filename, asyncIO)
			if err != nil {
				b.Fatalf("NewReaderWithAsyncIO failed: %v", err)
			}
			// Disable coalescing by setting maxMergeSize to 1 byte
			reader.maxMergeSize = 1
			reader.coalesceGap = -1

			_, err = reader.ReadRecordBatch()
			if err != nil {
				b.Fatalf("ReadRecordBatch failed: %v", err)
			}
			reader.Close()
		}
	})

	b.Run("coalesced", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			reader, err := NewReaderWithAsyncIO(filename, asyncIO)
			if err != nil {
				b.Fatalf("NewReaderWithAsyncIO failed: %v", err)
			}
			// Default coalescing settings (4KB gap, 1MB max)

			_, err = reader.ReadRecordBatch()
			if err != nil {
				b.Fatalf("ReadRecordBatch failed: %v", err)
			}
			reader.Close()
		}
	})
}


