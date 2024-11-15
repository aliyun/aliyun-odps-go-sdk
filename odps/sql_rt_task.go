package odps

import "encoding/xml"

type SQLRTTask struct {
	XMLName xml.Name `xml:"SQLRT"`
	BaseTask
}

func (t *SQLRTTask) TaskType() string {
	return "SQLRT"
}

func NewSqlRTTask(name string, comment string, hints map[string]string) SQLRTTask {
	return SQLRTTask{
		BaseTask: newBaseTask(name, comment, hints),
	}
}
