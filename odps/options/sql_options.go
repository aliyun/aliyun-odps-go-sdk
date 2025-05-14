package options

// CreateInstanceOptions holds the configuration for creating an instance.
type CreateInstanceOptions struct {
	Priority         int
	JobName          string // Job name
	TryWait          bool   // Try wait flag
	UniqueIdentifyID string // Unique identification ID
	MaxQAOptions     struct {
		UseMaxQA  bool
		QuotaName string
		SessionID string
	}
}

type CreateInstanceOption func(*CreateInstanceOptions)

// NewCreateInstanceOptions initializes a new instance of CreateInstanceOptions.
func NewCreateInstanceOptions(opts ...CreateInstanceOption) *CreateInstanceOptions {
	o := &CreateInstanceOptions{}
	for _, opt := range opts {
		opt(o)
	}
	return o
}

func WithPriority(priority int) CreateInstanceOption {
	return func(o *CreateInstanceOptions) {
		o.Priority = priority
	}
}

func WithUniqueIdentifyID(uniqueIdentifyID string) CreateInstanceOption {
	return func(o *CreateInstanceOptions) {
		o.UniqueIdentifyID = uniqueIdentifyID
	}
}

func WithJobName(jobName string) CreateInstanceOption {
	return func(o *CreateInstanceOptions) {
		o.JobName = jobName
	}
}

func WithTryWait(tryWait bool) CreateInstanceOption {
	return func(o *CreateInstanceOptions) {
		o.TryWait = tryWait
	}
}

func WithMaxQAOptions(sessionID string, quotaName string) CreateInstanceOption {
	return func(o *CreateInstanceOptions) {
		o.MaxQAOptions.UseMaxQA = true
		o.MaxQAOptions.QuotaName = quotaName
		o.MaxQAOptions.SessionID = sessionID
	}
}

// SQLTaskOptions holds the configuration for creating a SQLTask.
type SQLTaskOptions struct {
	TaskName       string
	InstanceOption *CreateInstanceOptions // Pointer to handle nil
	Hints          map[string]string
	Aliases        map[string]string
	Type           string
	DefaultSchema  string // Hint of "odps.default.schema" has a higher priority than this one
}

type SQLTaskOption func(*SQLTaskOptions)

// NewSQLTaskOptions initializes a new instance of SQLTaskOptions with default values.
func NewSQLTaskOptions(opts ...SQLTaskOption) *SQLTaskOptions {
	options := &SQLTaskOptions{
		TaskName: "AnonymousSQLTask",
		Type:     "sql",
		Hints:    make(map[string]string),
		Aliases:  make(map[string]string),
	}
	for _, opt := range opts {
		opt(options)
	}
	return options
}

func WithTaskName(name string) SQLTaskOption {
	return func(o *SQLTaskOptions) {
		o.TaskName = name
	}
}

func WithType(t string) SQLTaskOption {
	return func(o *SQLTaskOptions) {
		o.Type = t
	}
}

func WithDefaultSchema(schema string) SQLTaskOption {
	return func(o *SQLTaskOptions) {
		o.DefaultSchema = schema
	}
}

func WithHints(hints map[string]string) SQLTaskOption {
	return func(o *SQLTaskOptions) {
		o.Hints = make(map[string]string)
		for k, v := range hints {
			o.Hints[k] = v
		}
	}
}

func WithAliases(aliases map[string]string) SQLTaskOption {
	return func(o *SQLTaskOptions) {
		o.Aliases = make(map[string]string)
		for k, v := range aliases {
			o.Aliases[k] = v
		}
	}
}

func WithInstanceOption(opts ...CreateInstanceOption) SQLTaskOption {
	return func(o *SQLTaskOptions) {
		o.InstanceOption = NewCreateInstanceOptions(opts...)
	}
}
