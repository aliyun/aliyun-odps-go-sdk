package options

type OdpsOptions struct {
	LogViewVersion LogViewVersion
	RegionId       string
}

type OdpsOption func(*OdpsOptions)

// NewOdpsOptions initializes a new instance of OdpsOptions.
func NewOdpsOptions(opts ...OdpsOption) *OdpsOptions {
	o := &OdpsOptions{
		LogViewVersion: Auto,
		RegionId:       "cn",
	}
	for _, opt := range opts {
		opt(o)
	}
	return o
}

func WithRegionId(regionId string) OdpsOption {
	return func(o *OdpsOptions) {
		o.RegionId = regionId
	}
}

type LogViewVersion int

const (
	Auto LogViewVersion = iota
	LegacyLogView
	JobInsight
)

func WithLogViewVersion(version LogViewVersion) OdpsOption {
	return func(o *OdpsOptions) {
		o.LogViewVersion = version
	}
}
