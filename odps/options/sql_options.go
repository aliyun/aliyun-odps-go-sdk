package options

// CreateInstanceOption holds the configuration for creating an instance.
type CreateInstanceOption struct {
	ProjectName      string // Project name
	Priority         int    // Pointer to allow nil value
	JobName          string // Job name
	TryWait          bool   // Try wait flag
	UniqueIdentifyID string // Unique identification ID
}

// NewCreateInstanceOption initializes a new instance of CreateInstanceOption.
func NewCreateInstanceOption() *CreateInstanceOption {
	return &CreateInstanceOption{
		Priority: 9,
	}
}

// SQLTaskOption holds the configuration for creating a SQLTask.
type SQLTaskOption struct {
	TaskName       string
	InstanceOption *CreateInstanceOption // Pointer to handle nil
	Hints          map[string]string
	Aliases        map[string]string
	Type           string
	DefaultSchema  string // Hint of "odps.default.schema" has a higher priority than this one
}

// NewSQLTaskOption initializes a new instance of SQLTaskOption with default values.
func NewSQLTaskOption() *SQLTaskOption {
	return &SQLTaskOption{
		TaskName: "AnonymousSQLTask",
		Type:     "sql",
		Hints:    make(map[string]string),
		Aliases:  make(map[string]string),
	}
}
