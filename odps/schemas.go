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
	"encoding/xml"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/common"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/restclient"
	"github.com/pkg/errors"
	"net/url"
)

type Schemas struct {
	projectName string
	odpsIns     *Odps
}

// NewTables if projectName is not set，the default projectName of odps will be used
func NewSchemas(odpsIns *Odps, options ...string) *Schemas {
	var _projectName string

	if options == nil {
		_projectName = odpsIns.DefaultProjectName()
	} else {
		_projectName = options[0]
	}
	return &Schemas{
		projectName: _projectName,
		odpsIns:     odpsIns,
	}
}

// List get all the schemas
func (ts *Schemas) List(f func(*Schema, error)) {
	queryArgs := make(url.Values, 4)
	queryArgs.Set("expectmarker", "true")

	rb := common.ResourceBuilder{ProjectName: ts.projectName}
	resource := rb.Schemas()
	client := ts.odpsIns.restClient

	type ResModel struct {
		XMLName  xml.Name      `xml:"Schemas"`
		Schemas  []schemaModel `xml:"Schema"`
		Marker   string
		MaxItems int
	}

	var resModel ResModel
	for {
		err := client.GetWithModel(resource, queryArgs, &resModel)
		if err != nil {
			f(nil, err)
			break
		}

		for _, schemaModel := range resModel.Schemas {
			schema := NewSchema(ts.odpsIns, ts.projectName, schemaModel.Name)
			f(schema, nil)
		}

		if resModel.Marker != "" {
			queryArgs.Set("marker", resModel.Marker)
			resModel = ResModel{}
		} else {
			break
		}
	}
}

func (ts *Schemas) Get(schemaName string) *Schema {
	return NewSchema(ts.odpsIns, ts.projectName, schemaName)
}

func (ts *Schemas) Exists(schemaName string) (bool, error) {
	schema := ts.Get(schemaName)
	err := schema.Load()
	if err != nil {
		var noSuchObject restclient.NoSuchObject
		if errors.As(err, &noSuchObject) {
			return false, nil
		} else {
			return false, err
		}
	}
	return true, nil
}

func (ts *Schemas) Create(schemaName string, createIfNotExists bool, comment string) error {
	type createSchemaModel struct {
		XMLName     xml.Name `xml:"Schema"`
		Project     string
		Name        string
		Comment     string
		IfNotExists bool
	}
	postBodyModel := createSchemaModel{

		Project:     ts.projectName,
		Name:        schemaName,
		Comment:     comment,
		IfNotExists: createIfNotExists,
	}
	rb := common.ResourceBuilder{ProjectName: ts.projectName}
	resource := rb.Schemas()
	client := ts.odpsIns.restClient

	return client.DoXmlWithModel(
		common.HttpMethod.PostMethod,
		resource,
		nil,
		&postBodyModel,
		nil)
}

func (ts *Schemas) Delete(schemaName string) error {
	rb := common.ResourceBuilder{ProjectName: ts.projectName}
	resource := rb.Schema(schemaName)
	client := ts.odpsIns.restClient

	req, err := client.NewRequestWithUrlQuery(common.HttpMethod.DeleteMethod, resource, nil, nil)
	if err != nil {
		return err
	}
	return client.DoWithParseFunc(req, nil)
}
