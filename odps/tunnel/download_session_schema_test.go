package tunnel_test

import (
	"testing"

	"github.com/aliyun/aliyun-odps-go-sdk/odps/data"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/datatype"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/tableschema"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/tunnel"
	"github.com/stretchr/testify/assert"
)

// TestDownloadSessionWithSchema tests that download sessions work correctly with schema names
func TestDownloadSessionWithSchema(t *testing.T) {
	// Skip if no test environment is configured
	if odpsIns == nil {
		t.Skip("ODPS instance not configured for testing")
	}

	tableName := "download_session_schema_test"
	schemaName := SchemaName // Use the test schema from the environment

	// Create a test table in the specific schema
	tableSchema := tableschema.NewSchemaBuilder().Name(tableName).Column(
		tableschema.Column{Name: "c1", Type: datatype.BigIntType},
	).Column(tableschema.Column{Name: "c2", Type: datatype.StringType}).Build()

	// Clean up any existing table
	// Try to delete the table if it exists
	_ = odpsIns.Schema(schemaName).Tables().Delete(tableName, true)

	// Create the table in the specific schema
	err := odpsIns.Schema(schemaName).Tables().Create(tableSchema, true, nil, nil)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert some test data
	session, err := tunnelIns.CreateUploadSession(ProjectName, tableName,
		tunnel.SessionCfg.WithSchemaName(schemaName))
	if err != nil {
		t.Fatalf("Failed to create upload session: %v", err)
	}

	writer, err := session.OpenRecordWriter(0)
	if err != nil {
		t.Fatalf("Failed to open record writer: %v", err)
	}

	// Write test records
	for i := 0; i < 5; i++ {
		record := data.NewRecord(2)
		record[0] = data.BigInt(int64(i))
		record[1] = data.String("test" + string(rune('a'+i)))
		err = writer.Write(record)
		if err != nil {
			t.Fatalf("Failed to write record: %v", err)
		}
	}

	err = writer.Close()
	if err != nil {
		t.Fatalf("Failed to close writer: %v", err)
	}

	err = session.Commit([]int{0})
	if err != nil {
		t.Fatalf("Failed to commit upload: %v", err)
	}

	// Now test download session with schema
	downloadSession, err := tunnelIns.CreateDownloadSession(ProjectName, tableName,
		tunnel.SessionCfg.WithSchemaName(schemaName))
	if err != nil {
		t.Fatalf("Failed to create download session with schema: %v", err)
	}

	// Verify the session was created successfully
	assert.NotEmpty(t, downloadSession.Id)
	assert.Equal(t, schemaName, downloadSession.SchemaName)
	assert.Equal(t, tableName, downloadSession.TableName)

	// Check that we can get the record count
	recordCount := downloadSession.RecordCount()
	assert.GreaterOrEqual(t, recordCount, 0)

	// Test opening a record reader
	reader, err := downloadSession.OpenRecordReader(0, 5, nil)
	if err != nil {
		t.Fatalf("Failed to open record reader: %v", err)
	}

	// Read records and verify content
	count := 0
	for {
		record, err := reader.Read()
		if err != nil {
			break
		}
		if record == nil {
			break
		}

		assert.Equal(t, 2, record.Len())
		count++
	}

	assert.Equal(t, 5, count)

	err = reader.Close()
	if err != nil {
		t.Errorf("Failed to close reader: %v", err)
	}
}

// TestDownloadSessionResourceUrlWithSchema verifies that the ResourceUrl method
// generates correct URLs when schema name is provided
func TestDownloadSessionResourceUrlWithSchema(t *testing.T) {
	// Create a mock download session with schema name
	session := &tunnel.DownloadSession{
		ProjectName: "test-project",
		SchemaName:  "test-schema",
		TableName:   "test-table",
	}

	// Verify the resource URL includes the schema path
	expectedUrl := "/projects/test-project/schemas/test-schema/tables/test-table"
	actualUrl := session.ResourceUrl()
	assert.Equal(t, expectedUrl, actualUrl)
}

// TestDownloadSessionResourceUrlWithoutSchema verifies that the ResourceUrl method
// generates correct URLs when no schema name is provided
func TestDownloadSessionResourceUrlWithoutSchema(t *testing.T) {
	// Create a mock download session without schema name
	session := &tunnel.DownloadSession{
		ProjectName: "test-project",
		SchemaName:  "", // No schema
		TableName:   "test-table",
	}

	// Verify the resource URL does not include schema path
	expectedUrl := "/projects/test-project/tables/test-table"
	actualUrl := session.ResourceUrl()
	assert.Equal(t, expectedUrl, actualUrl)
}
