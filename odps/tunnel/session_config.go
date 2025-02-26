// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package tunnel

import (
	"strings"
)

type sessionConfig struct {
	PartitionKey string
	Overwrite    bool
	SchemaName   string
	Compressor   Compressor
	// ShardId is for download session only
	ShardId int
	// Async is for download session only
	Async bool
	// SlotNum for stream upload session only
	SlotNum int
	// CreatePartition for upload session (batch and stream) only
	CreatePartition bool
	// Columns for stream upload session only
	Columns       []string
	SchemaVersion int
	// AllowSchemaMismatch for stream upload session only
	AllowSchemaMismatch bool
}

func newSessionConfig(opts ...Option) *sessionConfig {
	cfg := &sessionConfig{
		Compressor:          nil,
		SchemaVersion:       -1,
		AllowSchemaMismatch: true,
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
		cfg.PartitionKey = strings.ReplaceAll(cfg.PartitionKey, "\"", "")
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

func withSlotNum(slotNum int) Option {
	return func(cfg *sessionConfig) {
		cfg.SlotNum = slotNum
	}
}

func withCreatePartition() Option {
	return func(cfg *sessionConfig) {
		cfg.CreatePartition = true
	}
}

func withColumns(c []string) Option {
	return func(cfg *sessionConfig) {
		cfg.Columns = c
	}
}

func withSchemaVersion(schemaVersion int) Option {
	return func(cfg *sessionConfig) {
		cfg.SchemaVersion = schemaVersion
	}
}

func withAllowSchemaMismatch(allowSchemaMismatch bool) Option {
	return func(cfg *sessionConfig) {
		cfg.AllowSchemaMismatch = allowSchemaMismatch
	}
}

var SessionCfg = struct {
	WithPartitionKey             func(string) Option
	WithSchemaName               func(string) Option
	WithDefaultDeflateCompressor func() Option
	WithDeflateCompressor        func(int) Option
	WithSnappyFramedCompressor   func() Option
	Overwrite                    func() Option
	WithShardId                  func(int) Option
	Async                        func() Option
	WithSlotNum                  func(int) Option
	WithCreatePartition          func() Option
	WithColumns                  func([]string) Option
	WithSchemaVersion            func(int) Option
	WithAllowSchemaMismatch      func(bool) Option
}{
	WithPartitionKey:             withPartitionKey,
	WithSchemaName:               withSchemaName,
	WithDefaultDeflateCompressor: withDefaultDeflateCompressor,
	WithDeflateCompressor:        withDeflateCompressor,
	WithSnappyFramedCompressor:   withSnappyFramedCompressor,
	Overwrite:                    overWrite,
	WithShardId:                  withShardId,
	Async:                        async,
	WithSlotNum:                  withSlotNum,
	WithCreatePartition:          withCreatePartition,
	WithColumns:                  withColumns,
	WithSchemaVersion:            withSchemaVersion,
	WithAllowSchemaMismatch:      withAllowSchemaMismatch,
}
