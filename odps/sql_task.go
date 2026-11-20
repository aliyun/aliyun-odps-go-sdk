// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package odps

import (
	"encoding/csv"
	"encoding/json"
	"encoding/xml"
	"io"
	"maps"
	"slices"
	"strings"

	"github.com/pkg/errors"

	"github.com/aliyun/aliyun-odps-go-sdk/odps/options"

	"github.com/aliyun/aliyun-odps-go-sdk/odps/common"
)

type SQLTask struct {
	XMLName  xml.Name `xml:"SQL"`
	TaskName `xml:"Name"`
	TaskConfig
	Query string
}

func NewAnonymousSQLTask(query string, hints map[string]string) SQLTask {
	return NewSqlTask("AnonymousSQLTask", query, hints)
}

func NewSqlTask(name string, query string, hints map[string]string) SQLTask {
	sqlTask := SQLTask{
		TaskName: TaskName(name),
		Query:    query,
	}

	sqlTask.Config = append(sqlTask.Config, common.Property{Name: "type", Value: "sql"})
	if hints != nil {
		hintsJson, _ := json.Marshal(hints)
		sqlTask.Config = append(sqlTask.Config, common.Property{Name: "settings", Value: string(hintsJson)})
	}
	return sqlTask
}

// NewSQLTaskWithOptions Create a SQLTask with options.SQLTaskOptions
func NewSQLTaskWithOptions(query string, option *options.SQLTaskOptions) SQLTask {
	if option == nil {
		return NewAnonymousSQLTask(query, nil)
	}
	taskName := option.TaskName
	if taskName == "" {
		taskName = "AnonymousSQLTask"
	}
	sqlTask := SQLTask{
		TaskName: TaskName(taskName),
		Query:    query,
	}
	taskType := option.Type
	if taskType == "" {
		taskType = "sql"
	}
	sqlTask.Config = append(sqlTask.Config, common.Property{Name: "type", Value: taskType})

	hints := option.Hints
	if hints["odps.default.schema"] == "" && option.DefaultSchema != "" {
		hints = make(map[string]string, len(option.Hints)+1)
		maps.Copy(hints, option.Hints)
		hints["odps.default.schema"] = option.DefaultSchema
	}
	if hints != nil {
		hintsJSON, _ := json.Marshal(hints)
		sqlTask.Config = append(sqlTask.Config, common.Property{Name: "settings", Value: string(hintsJSON)})
	}

	if option.Aliases != nil {
		aliasJSON, _ := json.Marshal(option.Aliases)
		sqlTask.Config = append(sqlTask.Config, common.Property{Name: "aliases", Value: string(aliasJSON)})
	}
	return sqlTask
}

func (t *SQLTask) TaskType() string {
	return "SQL"
}

func (t *SQLTask) RunInOdps(odpsIns *Odps, projectName string) (*Instance, error) {
	return t.Run(odpsIns, projectName)
}

func (t *SQLTask) Run(odpsIns *Odps, projectName string) (*Instance, error) {
	Instances := NewInstances(odpsIns)
	i, err := Instances.CreateTask(projectName, t)
	return i, errors.WithStack(err)
}

// GetSelectResultAsCsv reads up to 10,000 rows from the instance's inline task result.
func (t *SQLTask) GetSelectResultAsCsv(i *Instance, withColumnName bool) (*csv.Reader, error) {
	results, err := i.GetResult()
	if err != nil {
		return nil, errors.WithStack(err)
	}

	if len(results) == 0 {
		return nil, errors.Errorf("failed to get result from instance %s", i.Id())
	}
	// Legacy single-task responses may not identify the task by name.
	result := &results[0]
	if len(results) > 1 {
		index := slices.IndexFunc(results, func(result TaskResult) bool {
			return result.Name == t.GetName()
		})
		if index < 0 {
			return nil, errors.Errorf("instance %s returned no result for task %s", i.Id(), t.GetName())
		}
		result = &results[index]
	}
	content := result.Content()
	if result.Status != 0 && result.Status != TaskSuccess {
		return nil, taskStatusError(result.Name, result.Status, content)
	}
	reader := csv.NewReader(strings.NewReader(content))
	if !withColumnName {
		if _, err := reader.Read(); err != nil && err != io.EOF {
			return nil, errors.Wrap(err, "read CSV header")
		}
	}

	return reader, nil
}
