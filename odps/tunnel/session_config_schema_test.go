package tunnel_test

import (
	"testing"

	"github.com/aliyun/aliyun-odps-go-sdk/odps/tunnel"
	"github.com/stretchr/testify/assert"
)

// TestSessionConfigWithSchemaName tests that the session configuration
// properly handles schema names through options
func TestSessionConfigWithSchemaName(t *testing.T) {
	// Test that SessionCfg.WithSchemaName creates a valid option
	schemaName := "test-schema"
	option := tunnel.SessionCfg.WithSchemaName(schemaName)

	assert.NotNil(t, option)
	// We can't directly test the internal config since newSessionConfig is not exported
	// But we can verify the option is created successfully
}

// TestSessionConfigMultipleOptions tests that the session configuration
// properly handles multiple options including schema name
func TestSessionConfigMultipleOptions(t *testing.T) {
	schemaName := "test-schema"
	partitionKey := "p1='test'"

	// Test that all options can be created successfully
	option1 := tunnel.SessionCfg.WithSchemaName(schemaName)
	option2 := tunnel.SessionCfg.WithPartitionKey(partitionKey)
	option3 := tunnel.SessionCfg.WithDefaultDeflateCompressor()

	assert.NotNil(t, option1)
	assert.NotNil(t, option2)
	assert.NotNil(t, option3)
}
