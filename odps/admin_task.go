package odps

import (
	"encoding/xml"
)

type AdminTask struct {
	XMLName  xml.Name `xml:"Admin"`
	TaskName `xml:"Name"`
	// 注意: TaskConfig和Command的顺序不能更改
	TaskConfig
	Command string `xml:"Command"`
}

func NewAdminTask(name string, command string) *AdminTask {
	return &AdminTask{
		TaskName: TaskName(name),
		Command:  command,
	}
}

func (t *AdminTask) TaskType() string {
	return "AdminTask"
}
