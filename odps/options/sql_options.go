package options

// CreateInstanceOptions holds the configuration for creating an instance.
type CreateInstanceOptions struct {
	Priority         int    // Pointer to allow nil value
	JobName          string // Job name
	TryWait          bool   // Try wait flag
	UniqueIdentifyID string // Unique identification ID
}

// NewCreateInstanceOptions initializes a new instance of CreateInstanceOptions.
func NewCreateInstanceOptions() *CreateInstanceOptions {
	return &CreateInstanceOptions{
		Priority: 9,
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

// NewSQLTaskOptions initializes a new instance of SQLTaskOptions with default values.
func NewSQLTaskOptions() *SQLTaskOptions {
	return &SQLTaskOptions{
		TaskName: "AnonymousSQLTask",
		Type:     "sql",
		Hints:    make(map[string]string),
		Aliases:  make(map[string]string),
	}
}
