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
	"encoding/json"
	"encoding/xml"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/common"
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
	Config []common.Property `xml:"Config>Property"`
}

func (t *TaskConfig) AddProperty(key, value string) {
	t.Config = append(t.Config, common.Property{Name: key, Value: value})
}

type SQLCostTask struct {
	XMLName xml.Name `xml:"SQLCost"`
	SQLTask
}

func (t *SQLCostTask) TaskType() string {
	return "SQLCost"
}

func NewSQLCostTask(name string, query string, hints map[string]string) SQLCostTask {
	properties := make(map[string]string, 2)
	properties["sqlcostmode"] = "sqlcostmode"

	if hints != nil {
		hintsJson, _ := json.Marshal(hints)
		properties["settings"] = string(hintsJson)
	}

	sqlTask := NewSqlTask(name, query, properties)
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

type SQLRTTask struct {
	XMLName xml.Name `xml:"SQLRT"`
	SQLTask
}

func (t *SQLRTTask) TaskType() string {
	return "SQLRT"
}

type MergeTask struct {
	XMLName  xml.Name `xml:"Merge"`
	TaskName `xml:"Name"`
	Comment  string
	Tables   []string `xml:"Tables>TableName"`
	TaskConfig
}

func (t *MergeTask) TaskType() string {
	return "Merge"
}

func (t *MergeTask) AddTask(taskName string) {
	t.Tables = append(t.Tables, taskName)
}
