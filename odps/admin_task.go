package odps

import (
	"encoding/xml"
)

type AdminTask struct {
	XMLName  xml.Name `xml:"Admin"`
	TaskName `xml:"Name"`
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
