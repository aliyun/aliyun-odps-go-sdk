package odps

type TableIdentifier struct {
	ProjectName string
	SchemaName  string
	TableName   string
}

func (t *TableIdentifier) String() string {
	if t.ProjectName == "" {
		return t.TableName
	}
	if t.SchemaName == "" {
		return t.ProjectName + "." + t.TableName
	}
	return t.ProjectName + "." + t.SchemaName + "." + t.TableName
}

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
