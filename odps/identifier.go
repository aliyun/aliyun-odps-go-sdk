package odps

// TableIdentifier is used to uniquely identify a table in MaxCompute, compatible with both the three-layer model and the two-layer model.
type TableIdentifier struct {
	ProjectName string
	SchemaName  string
	TableName   string
}

// String returns the string representation of a TableIdentifier.
// For users with the Schema model enabled, the format is "ProjectName.SchemaName.TableName".
// For users who do not have it enabled, the format is "ProjectName.TableName".
func (t *TableIdentifier) String() string {
	if t.ProjectName == "" {
		return t.TableName
	}
	if t.SchemaName == "" {
		return t.ProjectName + "." + t.TableName
	}
	return t.ProjectName + "." + t.SchemaName + "." + t.TableName
}

// TableId generates a TableIdentifier instance based on the provided parameters.
// This function supports the following number of parameters:
// - 3 parameters: returns a TableIdentifier containing ProjectName, SchemaName, and TableName.
// - 2 parameters: returns a TableIdentifier containing ProjectName and TableName, with SchemaName empty.
// - 1 parameter: returns a TableIdentifier containing only TableName, with ProjectName and SchemaName empty.
// - 0 parameters: returns an empty TableIdentifier instance.
func TableId(option ...string) *TableIdentifier {
	if len(option) >= 3 {
		return &TableIdentifier{
			ProjectName: option[0],
			SchemaName:  option[1],
			TableName:   option[2],
		}
	} else if len(option) == 2 {
		return &TableIdentifier{
			ProjectName: option[0],
			SchemaName:  "",
			TableName:   option[1],
		}
	} else if len(option) == 1 {
		return &TableIdentifier{
			ProjectName: "",
			SchemaName:  "",
			TableName:   option[0],
		}
	} else {
		return &TableIdentifier{}
	}
}
