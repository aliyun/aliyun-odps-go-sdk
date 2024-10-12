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
	"github.com/aliyun/aliyun-odps-go-sdk/odps/common"
	"github.com/pkg/errors"
	"strings"
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

	// TODO add alias
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

// GetSelectResultAsCsv 最多返回1W条数据
func (t *SQLTask) GetSelectResultAsCsv(i *Instance, withColumnName bool) (*csv.Reader, error) {
	results, err := i.GetResult()
	if err != nil {
		return nil, errors.WithStack(err)
	}

	if len(results) <= 0 {
		return nil, errors.Errorf("failed to get result from instance %s", i.Id())
	}

	reader := csv.NewReader(strings.NewReader(results[0].Content()))
	if !withColumnName {
		_, _ = reader.Read()
	}

	return reader, nil
}
