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
	"bytes"
	"fmt"
	"strings"
	"text/template"

	"github.com/aliyun/aliyun-odps-go-sdk/arrow"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/common"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/datatype"
	"github.com/pkg/errors"
)

type TableSchema struct {
	TableName          string
	Columns            []Column
	Comment            string
	CreateTime         common.GMTTime
	ExtendedLabel      []string
	HubLifecycle       int
	IsExternal         bool
	IsMaterializedView bool
	IsVirtualView      bool
	LastDDLTime        common.GMTTime
	LastModifiedTime   common.GMTTime
	Lifecycle          int
	Owner              string
	PartitionColumns   []Column `json:"PartitionKeys"`
	RecordNum          int
	ShardExist         bool
	ShardInfo          string
	Size               int
	TableLabel         string
	ViewText           string

	// extended schema, got by adding "?extended" to table api
	FileNum      int
	IsArchived   bool
	PhysicalSize int
	Reserved     string // reserved json string, 字段不固定

	// for external table extended info
	StorageHandler string
	Location       string
	resources      string

	// for clustered info
	ClusterInfo ClusterInfo
}

// ClusterInfo 聚簇信息
type ClusterInfo struct {
	ClusterType string
	ClusterInfo []string
	SortCols    []SortColumn
}

type SortColumn struct {
	name  string
	order string
}

type SchemaBuilder struct {
	name             string
	comment          string
	columns          []Column
	partitionColumns []Column
	storageHandler   string
	location         string
	lifecycle        int
}

type ToArrowSchemaOption struct {
	WithPartitionColumns   bool
	WithExtensionTimeStamp bool
}

func NewSchemaBuilder() SchemaBuilder {
	return SchemaBuilder{}
}

func (builder *SchemaBuilder) Name(name string) *SchemaBuilder {
	builder.name = name
	return builder
}

func (builder *SchemaBuilder) Comment(comment string) *SchemaBuilder {
	builder.comment = comment
	return builder
}

func (builder *SchemaBuilder) Column(column Column) *SchemaBuilder {
	builder.columns = append(builder.columns, column)
	return builder
}

func (builder *SchemaBuilder) PartitionColumn(column Column) *SchemaBuilder {
	builder.partitionColumns = append(builder.partitionColumns, column)
	return builder
}

func (builder *SchemaBuilder) Columns(columns ...Column) *SchemaBuilder {
	builder.columns = append(builder.columns, columns...)
	return builder
}

func (builder *SchemaBuilder) PartitionColumns(columns ...Column) *SchemaBuilder {
	builder.partitionColumns = append(builder.partitionColumns, columns...)
	return builder
}

func (builder *SchemaBuilder) StorageHandler(storageHandler string) *SchemaBuilder {
	builder.storageHandler = storageHandler
	return builder
}

func (builder *SchemaBuilder) Location(location string) *SchemaBuilder {
	builder.location = location
	return builder
}

// Lifecycle 表的生命周期，仅支持正整数。单位：天
func (builder *SchemaBuilder) Lifecycle(lifecycle int) *SchemaBuilder {
	builder.lifecycle = lifecycle
	return builder
}

func (builder *SchemaBuilder) Build() TableSchema {
	return TableSchema{
		TableName:        builder.name,
		Columns:          builder.columns,
		PartitionColumns: builder.partitionColumns,
		Comment:          builder.comment,
		Lifecycle:        builder.lifecycle,
		StorageHandler:   builder.storageHandler,
		Location:         builder.location,
	}
}

func (schema *TableSchema) ToBaseSQLString(projectName string, createIfNotExists, isExternal bool) (string, error) {
	if schema.TableName == "" {
		return "", errors.New("table name is not set")
	}

	if len(schema.Columns) == 0 {
		return "", errors.New("table columns is not set")
	}

	var fns = template.FuncMap{
		"notLast": func(i, length int) bool {
			return i < length-1
		},
	}

	tplStr :=
		"{{$columnNum := len .Schema.Columns}}" +
			"{{$partitionNum := len .Schema.PartitionColumns}}" +
			"create {{if .IsExternal -}} external {{ end -}} table {{ if .CreateIfNotExists }}if not exists{{ end }} {{.ProjectName}}.`{{.Schema.TableName}}` (\n" +
			"{{ range $i, $column := .Schema.Columns  }}" +
			"    `{{.Name}}` {{.Type | print}} {{ if ne .Comment \"\" }}comment '{{.Comment}}'{{ end }}{{ if notLast $i $columnNum  }},{{ end }}\n" +
			"{{ end }}" +
			")" +
			"{{ if ne .Schema.Comment \"\"  }}" +
			"\ncomment '{{.Schema.Comment}}'" +
			"{{ end }}" +
			"{{ if gt $partitionNum 0 }}" +
			"\npartitioned by (" +
			"{{ range $i, $partition := .Schema.PartitionColumns }}" +
			"`{{.Name}}` {{.Type | print}} {{- if ne .Comment \"\" }} comment '{{.Comment}}' {{- end -}} {{- if notLast $i $partitionNum  }}, {{ end }}" +
			"{{ end -}}" +
			")" +
			"{{ end }}"

	tpl, err := template.New("DDL_CREATE_TABLE").Funcs(fns).Parse(tplStr)
	if err != nil {
		panic(err)
	}

	type Data struct {
		ProjectName       string
		Schema            *TableSchema
		IsExternal        bool
		CreateIfNotExists bool
	}

	data := Data{projectName, schema, isExternal, createIfNotExists}

	var out bytes.Buffer
	err = tpl.Execute(&out, data)
	if err != nil {
		panic(err)
	} else {
		return out.String(), nil
	}
}

func (schema *TableSchema) ToSQLString(projectName string, createIfNotExists bool) (string, error) {
	baseSql, err := schema.ToBaseSQLString(projectName, createIfNotExists, false)
	if err != nil {
		return "", errors.WithStack(err)
	}

	if schema.Lifecycle > 0 {
		baseSql += fmt.Sprintf("\nlifecycle %d", schema.Lifecycle)
	}

	baseSql += ";"

	return baseSql, nil
}

func (schema *TableSchema) ToExternalSQLString(
	projectName string,
	createIfNotExists bool,
	serdeProperties map[string]string,
	jars []string) (string, error) {

	if schema.StorageHandler == "" {
		return "", errors.New("TableSchema.StorageHandler is not set")
	}

	if schema.Location == "" {
		return "", errors.New("TableSchema.Location is not set")
	}

	baseSql, err := schema.ToBaseSQLString(projectName, createIfNotExists, true)

	if err != nil {
		return "", errors.WithStack(err)
	}

	var builder strings.Builder
	builder.WriteString(baseSql)

	// stored by, 用于指定自定义格式StorageHandler的类名或其他外部表文件格式
	builder.WriteString(fmt.Sprintf("\nstored by '%s'\n", schema.StorageHandler))

	// serde properties, 序列化属性参数
	if len(serdeProperties) > 0 {
		builder.WriteString("with serdeproperties(")
		i, n := 0, len(serdeProperties)

		for key, value := range serdeProperties {
			builder.WriteString(fmt.Sprintf("'%s'='%s'", key, value))
			i += 1
			if i < n {
				builder.WriteString(", ")
			}
		}

		builder.WriteString(")\n")
	}

	// location, 外部表存放地址
	builder.WriteString(fmt.Sprintf("location '%s'\n", schema.Location))

	// 自定义格式时类定义所在的jar
	if len(jars) > 0 {
		builder.WriteString("using '")
		n := len(jars)
		for i, jar := range jars {
			builder.WriteString(jar)

			if i < n-1 {
				builder.WriteString(", ")
			}
		}
		builder.WriteString("'\n")
	}

	if schema.Lifecycle > 0 {
		builder.WriteString(fmt.Sprintf("lifecycle %d", schema.Lifecycle))
	}

	builder.WriteRune(';')
	return builder.String(), nil
}

func (schema *TableSchema) ToArrowSchema(opt ...ToArrowSchemaOption) *arrow.Schema {
	fields := make([]arrow.Field, len(schema.Columns))
	//
	usingPartitionColumns := false
	usingExtensionTimeStamp := false
	if len(opt) == 1 {
		if opt[0].WithPartitionColumns {
			usingPartitionColumns = true
		}
		if opt[0].WithExtensionTimeStamp {
			usingExtensionTimeStamp = true
		}
	}
	//
	for i, column := range schema.Columns {
		arrowType, _ := TypeToArrowType(column.Type)
		metadata := arrow.NewMetadata(nil, nil)
		if column.Type.ID() == datatype.TIMESTAMP && usingExtensionTimeStamp {
			arrowType, _ = TypeToArrowType(column.Type, withExtensionTimeStamp())
			metadata = arrow.NewMetadata(
				[]string{"ARROW:extension:metadata", "ARROW:extension:name"},
				[]string{"odps_timestamp", "odps_timestamp"},
			)
		}
		fields[i] = arrow.Field{
			Name:     column.Name,
			Type:     arrowType,
			Nullable: column.IsNullable,
			Metadata: metadata,
		}
	}

	if schema.PartitionColumns != nil && usingPartitionColumns {
		for _, column := range schema.PartitionColumns {
			arrowType, _ := TypeToArrowType(column.Type)
			metadata := arrow.NewMetadata(nil, nil)
			if column.Type.ID() == datatype.TIMESTAMP && usingExtensionTimeStamp {
				arrowType, _ = TypeToArrowType(column.Type, withExtensionTimeStamp())
				metadata = arrow.NewMetadata(
					[]string{"ARROW:extension:metadata", "ARROW:extension:name"},
					[]string{"odps_timestamp", "odps_timestamp"},
				)
			}
			fields = append(fields, arrow.Field{
				Name:     column.Name,
				Type:     arrowType,
				Nullable: true, // todo: should be false, need tunnel backend fix this bug
				Metadata: metadata,
			})
		}
	}

	return arrow.NewSchema(fields, nil)
}

func (schema *TableSchema) FieldByName(name string) (Column, bool) {
	for _, c := range schema.Columns {
		if c.Name == name {
			return c, true
		}
	}

	return Column{}, false
}
