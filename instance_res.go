package odps

import (
	"encoding/xml"
	"strings"
)

type TaskStatus int

const (
	_ = iota
	TaskWaiting
	TaskRunning
	TaskSuccess
	TaskFailed
	TaskSuspended
	TaskCancelled
	TaskStatusUnknown
)

func TaskStatusFromStr(s string) TaskStatus {
	switch strings.ToUpper(s) {
	case "WAITING":
		return TaskWaiting
	case "RUNNING":
		return TaskRunning
	case "SUCCESS":
		return TaskSuccess
	case "FAILED":
		return TaskFailed
	case "SUSPENDED":
		return TaskSuspended
	case "CANCELLED":
		return TaskCancelled
	default:
		return TaskStatusUnknown
	}
}

func (status TaskStatus) String() string {
	switch status {
	case TaskWaiting:
		return "WAITING"
	case TaskRunning:
		return "RUNNING"
	case TaskSuccess:
		return "SUCCESS"
	case TaskFailed:
		return "FAILED"
	case TaskSuspended:
		return "SUSPENDED"
	case TaskCancelled:
		return "CANCELLED"
	default:
		return "TASK_STATUS_UNKNOWN"
	}
}

// TaskInInstance 通过Instance创建的Task
type TaskInInstance struct {
	Type      string `xml:"Type,attr"`
	Name      string
	StartTime GMTTime
	EndTime   GMTTime `xml:"EndTime"`
	Status    TaskStatus
}

func (status *TaskStatus) UnmarshalXML(d *xml.Decoder, start xml.StartElement) error {
	var s string

	if err := d.DecodeElement(&s, &start); err != nil {
		return err
	}

	*status = TaskStatusFromStr(s)

	return nil
}

func (status *TaskStatus) MarshalXML(d *xml.Encoder, start xml.StartElement) error {
	s := status.String()
	return d.EncodeElement(s, start)
}

type TaskProgressStage struct {
	ID                 string `xml:"ID,attr"`
	Status             string
	BackupWorkers      string
	TerminatedWorkers  string
	RunningWorkers     string
	TotalWorkers       string
	InputRecords       int
	OutRecords         int
	FinishedPercentage int
}

type TaskSummary struct {
	JsonSummary string
	Summary     string
}

type TaskResult struct {
	Type   string `xml:"Type,attr"`
	Name   string
	Result string
	Status TaskStatus
}
