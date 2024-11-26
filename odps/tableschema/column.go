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

package tableschema

import (
	"encoding/json"

	"github.com/pkg/errors"

	datatype2 "github.com/aliyun/aliyun-odps-go-sdk/odps/datatype"
)

type Column struct {
	Name               string
	Type               datatype2.DataType
	Comment            string
	Label              string
	IsNullable         bool
	HasDefaultValue    bool
	DefaultValue       string
	ExtendedLabels     []string
	GenerateExpression GenerateExpression
}

func (c *Column) UnmarshalJSON(data []byte) error {
	type ColumnShadow struct {
		Name                  string
		Type                  string
		Comment               string
		Label                 string
		IsNullable            bool
		HasDefaultValue       bool
		DefaultValue          string
		ExtendedLabels        []string
		HasGenerateExpression bool
		GenerateExpression    string
	}

	var cs ColumnShadow
	err := json.Unmarshal(data, &cs)
	if err != nil {
		return errors.WithStack(err)
	}

	_type, err := datatype2.ParseDataType(cs.Type)
	if err != nil {
		return errors.WithStack(err)
	}
	var _expression GenerateExpression
	if cs.HasGenerateExpression {
		_expression, err = parseGenerateExpression(cs.GenerateExpression)
		if err != nil {
			return errors.WithStack(err)
		}
	} else {
		_expression = nil
	}

	*c = Column{
		Name:               cs.Name,
		Type:               _type,
		Comment:            cs.Comment,
		Label:              cs.Label,
		IsNullable:         cs.IsNullable,
		HasDefaultValue:    cs.HasDefaultValue,
		DefaultValue:       cs.DefaultValue,
		ExtendedLabels:     cs.ExtendedLabels,
		GenerateExpression: _expression,
	}
	return nil
}
