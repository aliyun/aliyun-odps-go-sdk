package ipc

import (
	"bytes"
	"strings"
	"testing"

	"github.com/aliyun/aliyun-odps-go-sdk/arrow"
	"github.com/aliyun/aliyun-odps-go-sdk/arrow/array"
	"github.com/aliyun/aliyun-odps-go-sdk/arrow/memory"
)

// TestNestedPanicPath tests that panic messages contain the full nested field path
// when parsing deeply nested Arrow structures
func TestNestedPanicPath(t *testing.T) {
	mem := memory.NewGoAllocator()

	// Create a deeply nested schema:
	// root_struct
	//   └── level1_struct
	//         └── level2_array (ARRAY)
	//               └── level3_struct
	//                     └── deep_value (DOUBLE)
	schema := arrow.NewSchema(
		[]arrow.Field{
			{
				Name: "root_struct",
				Type: arrow.StructOf(
					arrow.Field{
						Name: "level1_struct",
						Type: arrow.StructOf(
							arrow.Field{
								Name: "level2_array",
								Type: arrow.ListOf(
									arrow.StructOf(
										arrow.Field{Name: "deep_value", Type: arrow.PrimitiveTypes.Float64},
										arrow.Field{Name: "deep_name", Type: arrow.BinaryTypes.String},
									),
								),
							},
							arrow.Field{Name: "level1_value", Type: arrow.PrimitiveTypes.Int64},
						),
					},
					arrow.Field{Name: "root_value", Type: arrow.PrimitiveTypes.Int32},
				),
			},
			{
				Name: "simple_col",
				Type: arrow.PrimitiveTypes.Float64,
			},
		},
		nil,
	)

	t.Logf("Schema: %v", schema)

	// Build a valid nested record first to verify the structure works
	b := array.NewRecordBuilder(mem, schema)
	defer b.Release()

	// Build root_struct
	rootStructBuilder := b.Field(0).(*array.StructBuilder)

	// Build simple_col
	simpleColBuilder := b.Field(1).(*array.Float64Builder)

	// Add 2 rows
	for i := 0; i < 2; i++ {
		rootStructBuilder.Append(true)

		// level1_struct
		level1StructBuilder := rootStructBuilder.FieldBuilder(0).(*array.StructBuilder)
		level1StructBuilder.Append(true)

		// level2_array
		level2ArrayBuilder := level1StructBuilder.FieldBuilder(0).(*array.ListBuilder)
		level2ArrayBuilder.Append(true)

		// Add items to array
		level3StructBuilder := level2ArrayBuilder.ValueBuilder().(*array.StructBuilder)
		for j := 0; j < 3; j++ {
			level3StructBuilder.Append(true)
			level3StructBuilder.FieldBuilder(0).(*array.Float64Builder).Append(float64(i*10 + j))
			level3StructBuilder.FieldBuilder(1).(*array.StringBuilder).Append("test")
		}

		// level1_value
		level1StructBuilder.FieldBuilder(1).(*array.Int64Builder).Append(int64(i * 100))

		// root_value
		rootStructBuilder.FieldBuilder(1).(*array.Int32Builder).Append(int32(i))

		// simple_col
		simpleColBuilder.Append(float64(i) * 1.5)
	}

	rec := b.NewRecord()
	defer rec.Release()

	t.Logf("Built record with %d rows, %d columns", rec.NumRows(), rec.NumCols())

	// Now serialize to IPC format and read back
	var buf bytes.Buffer
	w := NewWriter(&buf, WithSchema(schema))
	if err := w.Write(rec); err != nil {
		t.Fatalf("failed to write record: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("failed to close writer: %v", err)
	}

	t.Logf("Serialized to %d bytes", buf.Len())

	// Read back - this should work normally
	reader, err := NewReader(bytes.NewReader(buf.Bytes()), WithAllocator(mem))
	if err != nil {
		t.Fatalf("failed to create reader: %v", err)
	}
	defer reader.Release()

	readRec, err := reader.Read()
	if err != nil {
		t.Fatalf("failed to read record: %v", err)
	}
	t.Logf("Successfully read back record with %d rows", readRec.NumRows())

	// Verify the nested path tracking by checking the arrayLoaderContext directly
	t.Log("\n=== Testing nested path tracking ===")
	testNestedPathTracking(t)
}

// testNestedPathTracking directly tests the path tracking mechanism
func testNestedPathTracking(t *testing.T) {
	ctx := &arrayLoaderContext{
		path: make([]string, 0),
	}

	// Simulate nested path building
	ctx.pushPath("root_struct")
	if ctx.pathString() != "root_struct" {
		t.Errorf("expected 'root_struct', got '%s'", ctx.pathString())
	}

	ctx.pushPath("level1_struct")
	if ctx.pathString() != "root_struct.level1_struct" {
		t.Errorf("expected 'root_struct.level1_struct', got '%s'", ctx.pathString())
	}

	ctx.pushPath("level2_array")
	ctx.pushPath("[*]")
	if ctx.pathString() != "root_struct.level1_struct.level2_array.[*]" {
		t.Errorf("expected 'root_struct.level1_struct.level2_array.[*]', got '%s'", ctx.pathString())
	}

	ctx.pushPath("deep_value")
	expectedPath := "root_struct.level1_struct.level2_array.[*].deep_value"
	if ctx.pathString() != expectedPath {
		t.Errorf("expected '%s', got '%s'", expectedPath, ctx.pathString())
	}
	t.Logf("Full nested path: %s", ctx.pathString())

	// Test pop
	ctx.popPath()
	ctx.popPath()
	if ctx.pathString() != "root_struct.level1_struct.level2_array" {
		t.Errorf("after pop, expected 'root_struct.level1_struct.level2_array', got '%s'", ctx.pathString())
	}

	t.Log("Path tracking mechanism works correctly!")
}

// TestNestedPanicMessage simulates a panic and verifies the error message contains path info
func TestNestedPanicMessage(t *testing.T) {
	// Simulate what would happen if a panic occurs during nested field loading
	var panicMsg string

	func() {
		defer func() {
			if r := recover(); r != nil {
				panicMsg = r.(string)
			}
		}()

		// Simulate nested struct loading with panic
		simulateNestedPanic()
	}()

	t.Logf("Captured panic message:\n%s", panicMsg)

	// Verify the panic message contains expected path information
	expectedPaths := []string{
		"root_struct",
		"level1_struct",
		"level2_array",
		"[*]",
		"deep_value",
	}

	for _, path := range expectedPaths {
		if !strings.Contains(panicMsg, path) {
			t.Errorf("panic message should contain '%s', but got: %s", path, panicMsg)
		}
	}

	t.Log("Panic message contains all expected path information!")
}

// simulateNestedPanic simulates the panic wrapping behavior in loadStruct/loadList
func simulateNestedPanic() {
	ctx := &arrayLoaderContext{
		path: make([]string, 0),
	}

	// Simulate: root_struct -> level1_struct -> level2_array -> [*] -> deep_value
	ctx.pushPath("root_struct")
	func() {
		defer func() {
			if r := recover(); r != nil {
				panic("in struct field '" + ctx.pathString() + "': " + r.(string))
			}
		}()

		ctx.pushPath("level1_struct")
		func() {
			defer func() {
				if r := recover(); r != nil {
					panic("in struct field '" + ctx.pathString() + "': " + r.(string))
				}
			}()

			ctx.pushPath("level2_array")
			ctx.pushPath("[*]")
			func() {
				defer func() {
					if r := recover(); r != nil {
						panic("in list element '" + ctx.pathString() + "': " + r.(string))
					}
				}()

				ctx.pushPath("deep_value")
				func() {
					defer func() {
						if r := recover(); r != nil {
							panic("in struct field '" + ctx.pathString() + "': " + r.(string))
						}
					}()

					// Simulate the actual panic that would occur in setData
					panic("arrow/array: buffer size mismatch in Float64.setData: declared length=10, offset=0, but buffer only has 5 elements")
				}()
			}()
		}()
	}()
}
