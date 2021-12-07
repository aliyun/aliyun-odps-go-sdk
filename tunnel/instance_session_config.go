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
		QueryId: -1,
	}

	for _, opt := range opts {
		opt(cfg)
	}

	return cfg
}

// InstanceOption 不可直接使用，需要通过{@link InstanceSessionCfg}.XX构建
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
