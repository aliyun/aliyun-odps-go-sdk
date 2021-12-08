package odps

import (
	"encoding/csv"
	"encoding/xml"
	"fmt"
	"strings"
)

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

func (t *SQLTask) runInOdps(odpsIns *Odps, projectName string) (*Instance, error) {
	Instances := NewInstances(odpsIns)
	return Instances.CreateTask(projectName, t)
}

// GetSelectResultAsCsv 最多返回1W条数据
func (t *SQLTask) GetSelectResultAsCsv(i *Instance, withColumnName bool) (*csv.Reader, error) {
	results, err := i.GetResult()
	if err != nil {
		return nil, err
	}

	if len(results) <= 0 {
		return nil, fmt.Errorf("failed to get result from instance %s", i.Id())
	}

	reader := csv.NewReader(strings.NewReader(results[0].Result))
	if !withColumnName {
		_, _ = reader.Read()
	}

	return reader, nil
}
