package odps

import "encoding/xml"

type SQLTask struct {
	XMLName  xml.Name `xml:"SQL"`
	TaskName `xml:"Name"`
	Comment  string
	TaskConfig
	Query string
}

func NewSqlTask(name string, query string, comment string, properties map[string]string) SQLTask {
	sqlTask := SQLTask{
		TaskName: TaskName(name),
		Query:    query,
		Comment:  comment,
	}

	for key, value := range properties {
		sqlTask.Config = append(sqlTask.Config, Property{Name: key, Value: value})
	}

	return sqlTask
}

func (t *SQLTask) TaskType() string {
	return "SQL"
}
