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

type instanceSessionConfig struct {
	TaskName     string
	QueryId      int
	Compressor   Compressor
	LimitEnabled bool
}

func newInstanceSessionConfig(opts ...InstanceOption) *instanceSessionConfig {
	cfg := &instanceSessionConfig{
		LimitEnabled: false,
		QueryId:      -1,
	}

	for _, opt := range opts {
		opt(cfg)
	}

	return cfg
}

// InstanceOption must be created by InstanceSessionCfg.XXX
type InstanceOption func(cfg *instanceSessionConfig)

func withTaskName(taskName string) InstanceOption {
	return func(cfg *instanceSessionConfig) {
		cfg.TaskName = taskName
	}
}

func withQueryId(queryId int) InstanceOption {
	return func(cfg *instanceSessionConfig) {
		cfg.QueryId = queryId
	}
}

func enableLimit() InstanceOption {
	return func(cfg *instanceSessionConfig) {
		cfg.LimitEnabled = true
	}
}

func _withDefaultDeflateCompressor() InstanceOption {
	return func(cfg *instanceSessionConfig) {
		cfg.Compressor = defaultDeflate()
	}
}

func _withDeflateCompressor(level int) InstanceOption {
	return func(cfg *instanceSessionConfig) {
		cfg.Compressor = newDeflate(level)
	}
}

func _withSnappyFramedCompressor() InstanceOption {
	return func(cfg *instanceSessionConfig) {
		cfg.Compressor = newSnappyFramed()
	}
}

var InstanceSessionCfg = struct {
	WithTaskName                 func(string) InstanceOption
	WithQueryId                  func(int) InstanceOption
	WithDefaultDeflateCompressor func() InstanceOption
	WithDeflateCompressor        func(int) InstanceOption
	WithSnappyFramedCompressor   func() InstanceOption
	EnableLimit                  func() InstanceOption
}{
	WithTaskName:                 withTaskName,
	WithQueryId:                  withQueryId,
	WithDefaultDeflateCompressor: _withDefaultDeflateCompressor,
	WithDeflateCompressor:        _withDeflateCompressor,
	WithSnappyFramedCompressor:   _withSnappyFramedCompressor,
	EnableLimit:                  enableLimit,
}
