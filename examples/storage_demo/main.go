// Storage Demo
// This example demonstrates the low-level columnar storage API.
// Shows how to write and read vectors using the Lance format.
//
// Run: go run main.go
package main

import (
	"fmt"
	"os"
	"time"

	"github.com/wzqhbustb/vego/storage/arrow"
	"github.com/wzqhbustb/vego/storage/column"
	"github.com/wzqhbustb/vego/storage/encoding"
)

func main() {
	fmt.Println("=== Vego Columnar Storage Demo ===")
	fmt.Println()

	// Create temp directory
	tmpDir := "/tmp/vego_storage_demo"
	os.MkdirAll(tmpDir, 0755)
	defer os.RemoveAll(tmpDir)
	fmt.Printf("Working directory: %s\n", tmpDir)
	fmt.Println()

	dimension := 128
	numVectors := 1000

	// Step 1: Define schema
	fmt.Println("Step 1: Defining Arrow schema...")
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimInt64(), Nullable: false},
		{Name: "vector", Type: arrow.VectorType(dimension), Nullable: false},
		{Name: "score", Type: arrow.PrimFloat32(), Nullable: false},
	}, nil)
	fmt.Printf("✓ Schema created with %d fields:\n", schema.NumFields())
	for i := 0; i < schema.NumFields(); i++ {
		field := schema.Field(i)
		fmt.Printf("  - %s: %s\n", field.Name, field.Type.Name())
	}
	fmt.Println()

	// Step 2: Build data arrays
	fmt.Printf("Step 2: Building data arrays (%d vectors)...\n", numVectors)
	
	idBuilder := arrow.NewInt64Builder()
	vectorBuilder := arrow.NewFixedSizeListBuilder(
		arrow.FixedSizeListOf(arrow.PrimFloat32(), dimension).(*arrow.FixedSizeListType),
	)
	scoreBuilder := arrow.NewFloat32Builder()

	for i := 0; i < numVectors; i++ {
		// ID
		idBuilder.Append(int64(i))
		
		// Vector (simple pattern for demo)
		vec := make([]float32, dimension)
		for j := range vec {
			vec[j] = float32(i) * 0.001 + float32(j) * 0.0001
		}
		vectorBuilder.AppendValues(vec)
		
		// Score
		scoreBuilder.Append(float32(i) * 0.1)
	}

	idArray := idBuilder.NewArray()
	vectorArray := vectorBuilder.NewArray()
	scoreArray := scoreBuilder.NewArray()
	
	fmt.Printf("✓ Built arrays:\n")
	fmt.Printf("  - IDs: %d values\n", idArray.Len())
	fmt.Printf("  - Vectors: %d values (dimension %d)\n", vectorArray.Len(), dimension)
	fmt.Printf("  - Scores: %d values\n", scoreArray.Len())
	fmt.Println()

	// Step 3: Create RecordBatch
	fmt.Println("Step 3: Creating RecordBatch...")
	batch, err := arrow.NewRecordBatch(schema, numVectors, []arrow.Array{
		idArray, vectorArray, scoreArray,
	})
	if err != nil {
		panic(err)
	}
	fmt.Printf("✓ RecordBatch created with %d rows\n", batch.NumRows())
	fmt.Println()

	// Step 4: Write to Lance file
	fmt.Println("Step 4: Writing to Lance file...")
	filename := tmpDir + "/vectors.lance"
	factory := encoding.NewEncoderFactory(3) // Compression level 3
	
	writer, err := column.NewWriter(filename, schema, factory)
	if err != nil {
		panic(err)
	}

	start := time.Now()
	err = writer.WriteRecordBatch(batch)
	if err != nil {
		panic(err)
	}
	writer.Close()
	elapsed := time.Since(start)

	fmt.Printf("✓ Written to %s\n", filename)
	fmt.Printf("  - Write time: %v\n", elapsed)
	
	// Check file stats
	if info, err := os.Stat(filename); err == nil {
		fmt.Printf("  - File size: %.2f KB\n", float64(info.Size())/1024)
		originalSize := numVectors * (8 + dimension*4 + 4) // id + vector + score
		fmt.Printf("  - Original size: %.2f KB\n", float64(originalSize)/1024)
		fmt.Printf("  - Compression ratio: %.1f%%\n", 100.0*float64(info.Size())/float64(originalSize))
	}
	fmt.Println()

	// Step 5: Read from Lance file
	fmt.Println("Step 5: Reading from Lance file...")
	
	reader, err := column.NewReader(filename)
	if err != nil {
		panic(err)
	}
	defer reader.Close()

	start = time.Now()
	resultBatch, err := reader.ReadRecordBatch()
	if err != nil {
		panic(err)
	}
	elapsed = time.Since(start)

	fmt.Printf("✓ Read from %s\n", filename)
	fmt.Printf("  - Read time: %v\n", elapsed)
	fmt.Printf("  - Rows read: %d\n", resultBatch.NumRows())
	fmt.Println()

	// Step 6: Access data
	fmt.Println("Step 6: Accessing data...")
	resultIDArray := resultBatch.Column(0).(*arrow.Int64Array)
	resultVectorArray := resultBatch.Column(1).(*arrow.FixedSizeListArray)
	resultScoreArray := resultBatch.Column(2).(*arrow.Float32Array)

	// Show first 5 rows
	fmt.Printf("First 5 rows:\n")
	vectorValues := resultVectorArray.Values().(*arrow.Float32Array)
	for i := 0; i < 5; i++ {
		id := resultIDArray.Value(i)
		score := resultScoreArray.Value(i)
		
		// Get vector (first 3 values)
		start := i * dimension
		vec0 := vectorValues.Value(start)
		vec1 := vectorValues.Value(start + 1)
		vec2 := vectorValues.Value(start + 2)
		
		fmt.Printf("  Row %d: ID=%d, Score=%.2f, Vector[0..2]=[%.4f, %.4f, %.4f]\n",
			i, id, score, vec0, vec1, vec2)
	}
	fmt.Println()

	// Show last 5 rows
	fmt.Printf("Last 5 rows:\n")
	for i := numVectors - 5; i < numVectors; i++ {
		id := resultIDArray.Value(i)
		score := resultScoreArray.Value(i)
		
		start := i * dimension
		vec0 := vectorValues.Value(start)
		vec1 := vectorValues.Value(start + 1)
		vec2 := vectorValues.Value(start + 2)
		
		fmt.Printf("  Row %d: ID=%d, Score=%.2f, Vector[0..2]=[%.4f, %.4f, %.4f]\n",
			i, id, score, vec0, vec1, vec2)
	}
	fmt.Println()

	fmt.Println("=== Demo completed successfully! ===")
}
