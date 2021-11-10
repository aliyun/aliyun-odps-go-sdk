package odps

import (
	"encoding/json"
	"encoding/xml"
)

type Task interface {
	GetName() string
	TaskType() string
	AddProperty(key, value string)
}

// TaskName 作为embedding filed使用时，使用者自动实现Task接口的GetName方法
type TaskName string

func (n TaskName) GetName() string {
	return string(n)
}

// TaskConfig 作为embedding filed使用时，使用者自动实现Task接口的AddProperty方法
type TaskConfig struct {
	Config []Property `xml:"Config>Property"`
}

func (t *TaskConfig) AddProperty(key, value string) {
	t.Config = append(t.Config, Property{Name: key, Value: value})
}

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

type SQLCostTask struct {
	XMLName xml.Name `xml:"SQLCost"`
	SQLTask
}

func (t *SQLCostTask) TaskType() string {
	return "SQLCost"
}

func NewSQLCostTask(name string, query string, comment string, hints map[string]string) SQLCostTask {
	properties := make(map[string]string, 2)
	properties["sqlcostmode"] = "sqlcostmode"

	if hints != nil {
		hintsJson, _ := json.Marshal(hints)
		properties["settings"] = string(hintsJson)
	}

	sqlTask := NewSqlTask(name, query, comment, properties)
	var sqlCostTask SQLCostTask
	sqlCostTask.SQLTask = sqlTask

	return sqlCostTask
}

type SQLPlanTask struct {
	XMLName xml.Name `xml:"SQLPlan"`
	SQLTask
}

func (t *SQLPlanTask) TaskType() string {
	return "SQLPlan"
}

type MergeTask struct {
	XMLName  xml.Name `xml:"Merge"`
	TaskName `xml:"Name"`
	Comment  string
	Tables   []string `xml:"Tables>TableName"`
	TaskConfig
}

func (t *MergeTask) TaskType() string  {
	return "Merge"
}

func (t *MergeTask) AddTask(taskName string) {
	t.Tables = append(t.Tables, taskName)
}
