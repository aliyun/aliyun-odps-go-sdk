package tunnel

import (
	"strings"
)

type sessionConfig struct {
	PartitionKey string
	Overwrite    bool
	SchemaName   string
	Compressor   Compressor
	UseArrow     bool
	// ShardId is for download session only
	ShardId int
	// Async is for download session only
	Async bool
}

func newSessionConfig(opts ...Option) *sessionConfig {
	cfg := &sessionConfig{
		UseArrow:   true,
		Compressor: nil,
	}

	for _, opt := range opts {
		opt(cfg)
	}

	return cfg
}

// Option can not be used directly, it can be created by SessionCfg.XXX
type Option func(cfg *sessionConfig)

func withPartitionKey(partitionKey string) Option {
	return func(cfg *sessionConfig) {
		cfg.PartitionKey = strings.ReplaceAll(partitionKey, "'", "")
	}
}

func withSchemaName(schemaName string) Option {
	return func(cfg *sessionConfig) {
		cfg.SchemaName = schemaName
	}
}

func withDefaultDeflateCompressor() Option {
	return func(cfg *sessionConfig) {
		cfg.Compressor = defaultDeflate()
	}
}

func withDeflateCompressor(level int) Option {
	return func(cfg *sessionConfig) {
		cfg.Compressor = newDeflate(level)
	}
}

func withSnappyFramedCompressor() Option {
	return func(cfg *sessionConfig) {
		cfg.Compressor = newSnappyFramed()
	}
}

func useArrow() Option {
	return func(cfg *sessionConfig) {
		cfg.UseArrow = true
	}
}

func overWrite() Option {
	return func(cfg *sessionConfig) {
		cfg.Overwrite = true
	}
}

func async() Option {
	return func(cfg *sessionConfig) {
		cfg.Async = true
	}
}

func withShardId(shardId int) Option {
	return func(cfg *sessionConfig) {
		cfg.ShardId = shardId
	}
}

var SessionCfg = struct {
	WithPartitionKey             func(string) Option
	WithSchemaName               func(string) Option
	WithDefaultDeflateCompressor func() Option
	WithDeflateCompressor        func(int) Option
	WithSnappyFramedCompressor   func() Option
	Overwrite                    func() Option
	UseArrow                     func() Option
	WithShardId                  func(int) Option
	Async                        func() Option
}{
	WithPartitionKey:             withPartitionKey,
	WithSchemaName:               withSchemaName,
	WithDefaultDeflateCompressor: withDefaultDeflateCompressor,
	WithDeflateCompressor:        withDeflateCompressor,
	WithSnappyFramedCompressor:   withSnappyFramedCompressor,
	Overwrite:                    overWrite,
	UseArrow:                     useArrow,
	WithShardId:                  withShardId,
	Async:                        async,
}
