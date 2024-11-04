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
	"net/url"

	"github.com/aliyun/aliyun-odps-go-sdk/odps/common"
)

type Schemas struct {
	projectName string
	odpsIns     *Odps
}

// NewSchemas if projectName is not set，the default projectName of odps will be used
func NewSchemas(odpsIns *Odps, projectName string) *Schemas {
	if projectName == "" {
		projectName = odpsIns.DefaultProjectName()
	}
	return &Schemas{
		projectName: projectName,
		odpsIns:     odpsIns,
	}
}

// List get all the schemas
func (ss *Schemas) List(f func(*Schema, error)) error {
	queryArgs := make(url.Values, 4)
	queryArgs.Set("expectmarker", "true")

	rb := common.ResourceBuilder{ProjectName: ss.projectName}
	resource := rb.Schemas()
	client := ss.odpsIns.restClient

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
			return err
		}

		for _, schemaModel := range resModel.Schemas {
			schema := NewSchema(ss.odpsIns, ss.projectName, schemaModel.Name)
			f(schema, nil)
		}

		if resModel.Marker != "" {
			queryArgs.Set("marker", resModel.Marker)
			resModel = ResModel{}
		} else {
			break
		}
	}

	return nil
}

// Get the schema
func (ss *Schemas) Get(schemaName string) *Schema {
	return NewSchema(ss.odpsIns, ss.projectName, schemaName)
}

// Create the schema
func (ss *Schemas) Create(schemaName string, createIfNotExists bool, comment string) error {
	type createSchemaModel struct {
		XMLName     xml.Name `xml:"Schema"`
		Project     string
		Name        string
		Comment     string
		IfNotExists bool
	}
	postBodyModel := createSchemaModel{

		Project:     ss.projectName,
		Name:        schemaName,
		Comment:     comment,
		IfNotExists: createIfNotExists,
	}
	rb := common.ResourceBuilder{ProjectName: ss.projectName}
	resource := rb.Schemas()
	client := ss.odpsIns.restClient

	return client.DoXmlWithModel(
		common.HttpMethod.PostMethod,
		resource,
		nil,
		&postBodyModel,
		nil)
}

// Delete the schema
func (ss *Schemas) Delete(schemaName string) error {
	rb := common.ResourceBuilder{ProjectName: ss.projectName}
	resource := rb.Schema(schemaName)
	client := ss.odpsIns.restClient

	req, err := client.NewRequestWithUrlQuery(common.HttpMethod.DeleteMethod, resource, nil, nil)
	if err != nil {
		return err
	}
	return client.DoWithParseFunc(req, nil)
}
