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
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"text/template"

	"github.com/aliyun/aliyun-odps-go-sdk/arrow"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/common"
	"github.com/pkg/errors"
)

type TableSchema struct {
	TableName                        string
	Columns                          []Column
	Comment                          string
	CreateTime                       common.GMTTime
	ExtendedLabel                    []string
	HubLifecycle                     int
	IsExternal                       bool
	IsMaterializedView               bool
	IsMaterializedViewRewriteEnabled bool
	IsMaterializedViewOutdated       bool

	IsVirtualView    bool
	LastDDLTime      common.GMTTime
	LastModifiedTime common.GMTTime
	LastAccessTime   common.GMTTime
	Lifecycle        int
	Owner            string
	PartitionColumns []Column `json:"PartitionKeys"`
	RecordNum        int
	ShardExist       bool
	ShardInfo        string
	Size             int64
	TableLabel       string
	ViewText         string
	ViewExpandedText string

	// extended schema, got by adding "?extended" to table api
	FileNum      int
	IsArchived   bool
	PhysicalSize int
	Reserved     string // reserved json string, 字段不固定

	// for external table extended info
	StorageHandler  string
	Location        string
	resources       string
	SerDeProperties map[string]string `json:"-"`
	Props           string
	MvProperties    map[string]string `json:"-"` // materialized view properties
	RefreshHistory  string

	// for clustered info
	ClusterInfo ClusterInfo
}

type ClusterType = string

// ClusterInfo 聚簇信息
type ClusterInfo struct {
	ClusterType ClusterType
	ClusterCols []string
	SortCols    []SortColumn
	BucketNum   int
}

var CLUSTER_TYPE = struct {
	Hash  ClusterType
	Range ClusterType
}{
	Hash:  "hash",
	Range: "range",
}

type SortOrder string

var SORT_ORDER = struct {
	ASC  SortOrder
	DESC SortOrder
}{
	ASC:  "asc",
	DESC: "desc",
}

type SortColumn struct {
	Name  string
	Order SortOrder
}

type SchemaBuilder struct {
	name                             string
	comment                          string
	columns                          []Column
	partitionColumns                 []Column
	storageHandler                   string
	location                         string
	viewText                         string
	lifecycle                        int
	clusterInfo                      ClusterInfo
	isVirtualView                    bool
	isMaterializedView               bool
	isMaterializedViewRewriteEnabled bool
	mvProperties                     map[string]string
}

func NewSchemaBuilder() *SchemaBuilder {
	return &SchemaBuilder{}
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

func (builder *SchemaBuilder) ClusterType(clusterType ClusterType) *SchemaBuilder {
	builder.clusterInfo.ClusterType = clusterType
	return builder
}

func (builder *SchemaBuilder) ClusterColumns(clusterCols []string) *SchemaBuilder {
	builder.clusterInfo.ClusterCols = clusterCols
	return builder
}

func (builder *SchemaBuilder) ClusterSortColumns(clusterSortCols []SortColumn) *SchemaBuilder {
	builder.clusterInfo.SortCols = clusterSortCols
	return builder
}

func (builder *SchemaBuilder) ClusterBucketNum(bucketNum int) *SchemaBuilder {
	builder.clusterInfo.BucketNum = bucketNum
	return builder
}

func (builder *SchemaBuilder) IsMaterializedView(isMaterializedView bool) *SchemaBuilder {
	builder.isMaterializedView = isMaterializedView
	return builder
}

func (builder *SchemaBuilder) IsMaterializedViewRewriteEnabled(isMaterializedViewRewriteEnabled bool) *SchemaBuilder {
	builder.isMaterializedViewRewriteEnabled = isMaterializedViewRewriteEnabled
	return builder
}

func (builder *SchemaBuilder) IsVirtualView(isVirtualView bool) *SchemaBuilder {
	builder.isVirtualView = isVirtualView
	return builder
}

func (builder *SchemaBuilder) ViewText(viewText string) *SchemaBuilder {
	builder.viewText = viewText
	return builder
}

func (builder *SchemaBuilder) MvProperty(key, value string) *SchemaBuilder {
	if builder.mvProperties == nil {
		builder.mvProperties = make(map[string]string)
	}
	builder.mvProperties[key] = value
	return builder
}

func (builder *SchemaBuilder) MvProperties(properties map[string]string) *SchemaBuilder {
	builder.mvProperties = properties
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
		ClusterInfo:      builder.clusterInfo,

		IsVirtualView:                    builder.isVirtualView,
		IsMaterializedView:               builder.isMaterializedView,
		IsMaterializedViewRewriteEnabled: builder.isMaterializedViewRewriteEnabled,
		ViewText:                         builder.viewText,
		MvProperties:                     builder.mvProperties,
	}
}

func (schema *TableSchema) UnmarshalJSON(data []byte) error {
	type Alias TableSchema
	tempSchema := &struct {
		SerDePropertiesString *string `json:"serDeProperties,omitempty"`
		MvPropertiesString    *string `json:"mvProperties,omitempty"`
		*Alias
	}{
		Alias: (*Alias)(schema),
	}
	if err := json.Unmarshal(data, &tempSchema); err != nil {
		return err
	}
	if tempSchema.SerDePropertiesString != nil {
		err := json.Unmarshal([]byte(*tempSchema.SerDePropertiesString), &schema.SerDeProperties)
		if err != nil {
			return err
		}
	}
	if tempSchema.MvPropertiesString != nil {
		err := json.Unmarshal([]byte(*tempSchema.MvPropertiesString), &schema.MvProperties)
		if err != nil {
			return err
		}
	}
	return nil
}

func (schema *TableSchema) ToBaseSQLString(projectName string, schemaName string, createIfNotExists, isExternal bool) (string, error) {
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
			"create {{if .IsExternal -}} external {{ end -}} table {{ if .CreateIfNotExists }}if not exists{{ end }} " +
			"{{.ProjectName}}.{{if ne .SchemaName \"\"}}`{{.SchemaName}}`.{{end}}`{{.Schema.TableName}}` (\n" +
			"{{ range $i, $column := .Schema.Columns  }}" +
			"    `{{.Name}}` {{.Type.Name | print}} {{ if ne .Comment \"\" }}comment '{{.Comment}}'{{ end }}{{ if notLast $i $columnNum  }},{{ end }}\n" +
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
		SchemaName        string
		Schema            *TableSchema
		IsExternal        bool
		CreateIfNotExists bool
	}

	data := Data{projectName, schemaName, schema, isExternal, createIfNotExists}

	var out bytes.Buffer
	err = tpl.Execute(&out, data)
	if err != nil {
		panic(err)
	}
	return out.String(), nil
}

func (schema *TableSchema) ToViewSQLString(projectName string, schemaName string, orReplace, createIfNotExists, buildDeferred bool) (string, error) {
	if schema.TableName == "" {
		return "", errors.New("view name is not set")
	}

	if (schema.IsVirtualView || schema.IsMaterializedView) && schema.ViewText == "" {
		return "", errors.New("view text is not set")
	}

	if schema.IsVirtualView && len(schema.PartitionColumns) > 0 {
		return "", errors.New("virtual view can not have partition columns")
	}

	if schema.IsVirtualView && schema.IsMaterializedView {
		return "", errors.New("virtual view and materialized view can not be both set")
	}

	if !schema.IsVirtualView && !schema.IsMaterializedView {
		return "", errors.New("either virtual view or materialized should be set")
	}

	var fns = template.FuncMap{
		"notLast": func(i, length int) bool {
			return i < length-1
		},
	}

	tplStr := "{{$columnNum := len .Schema.Columns}}" +
		"{{$partitionNum := len .Schema.PartitionColumns}}" +
		"{{$mvPropertiesNum:= len .Schema.MvProperties}}" +
		"{{$clusterColsNum:= len .Schema.ClusterInfo.ClusterCols}}" +
		"{{$clusterSortColsNum:= len .Schema.ClusterInfo.SortCols}}" +
		"create " +
		"{{if and .Schema.IsVirtualView .OrReplace -}}" +
		"or replace  " +
		"{{end}}" +
		"{{if .Schema.IsMaterializedView -}}" +
		"materialized  " +
		"{{ end -}}" +
		"view " +
		"{{ if .CreateIfNotExists }}" +
		"if not exists" +
		"{{ end }} " +
		"{{if ne .ProjectName \"\"}}" +
		"{{.ProjectName}}." +
		"{{end}}" +
		"{{if ne .SchemaName \"\"}}" +
		"`{{.SchemaName}}`." +
		"{{end}}" +
		"`{{.Schema.TableName}}`" +
		"{{ if .Schema.IsMaterializedView }}" +
		"{{ if gt .Schema.Lifecycle 0 }}" +
		"\nlifecycle {{.Schema.Lifecycle}}" +
		" {{end -}}" +
		"{{ if .BuildDeferred }}" +
		"\nbuild deferred" +
		" {{end -}}" +
		"{{end -}}" +
		"{{ if gt $columnNum 0 }}" +
		"(\n {{ range $i, $column := .Schema.Columns  }}" +
		"    `{{.Name}}` {{ if ne .Comment \"\" }}comment '{{.Comment}}'{{ end }}{{ if notLast $i $columnNum  }},{{ end }}\n" +
		"{{ end }}" +
		")" +
		"{{ end }}" +
		"{{ if .Schema.IsMaterializedView }}" +
		"{{ if not .Schema.IsMaterializedViewRewriteEnabled}}" +
		" disable rewrite " +
		"{{end -}}" +
		"{{end -}}" +
		"{{ if ne .Schema.Comment \"\"  }}" +
		"\ncomment '{{.Schema.Comment}}'" +
		"{{ end }}" +
		"{{ if .Schema.IsMaterializedView }}" +
		"{{ if gt $partitionNum 0 }}" +
		"\npartitioned by (" +
		"{{ range $i, $partition := .Schema.PartitionColumns }}" +
		"\n`{{.Name}}`" +
		" {{- if notLast $i $partitionNum  }}" +
		"," +
		" {{ end }}" +
		"{{ end -}}" +
		")" +
		"{{ end }}" +
		"{{ if gt $clusterColsNum 0 }}" +
		"{{ if eq .Schema.ClusterInfo.ClusterType \"hash\" }}" +
		" \nclustered by (" +
		"{{ range $i, $clusterCol := .Schema.ClusterInfo.ClusterCols }}" +
		"`{{.}}`" +
		"{{ if notLast $i $clusterColsNum  }},{{ end }}" +
		"{{end -}}" +
		")" +
		"{{else if eq .Schema.ClusterInfo.ClusterType \"range\"}}" +
		"\nrange clustered by (" +
		"{{ range $i, $clusterCol := .Schema.ClusterInfo.ClusterCols }}" +
		"`{{.}}`" +
		"{{ if notLast $i $clusterColsNum  }},{{ end }}" +
		"{{end -}}" +
		")" +
		"{{end -}}" +
		"{{ if gt $clusterSortColsNum 0 }}" +
		"\nsorted by (" +
		"{{ range $i, $clusterSortCol := .Schema.ClusterInfo.SortCols }}" +
		"`{{.Name}}` {{.Order}}" +
		"{{ if notLast $i $clusterSortColsNum  }},{{ end }}" +
		"{{end -}}" +
		")" +
		"{{end -}}" +
		"{{ if .Schema.ClusterInfo.BucketNum }}" +
		" into {{.Schema.ClusterInfo.BucketNum}} buckets" +
		"{{end -}}" +
		"{{end -}}" +
		"{{ if .MvProperties }} " +
		"\nTBLPROPERTIES (" +
		"{{ range $i, $kv := .MvProperties }}" +
		"\"{{$kv.Key}}\"=\"{{$kv.Value}}\"" +
		"{{ if notLast $i $mvPropertiesNum  }},{{ end }}" +
		"{{end -}}" +
		")" +
		"{{end}}" +
		"{{end}}" +
		"\nas {{.Schema.ViewText}};"

	tpl, err := template.New("DDL_CREATE_VIEW").Funcs(fns).Parse(tplStr)
	if err != nil {
		panic(err)
	}

	type kvPair struct {
		Key   string
		Value string
	}

	type Data struct {
		ProjectName       string
		SchemaName        string
		Schema            *TableSchema
		MvProperties      []kvPair
		OrReplace         bool
		CreateIfNotExists bool
		BuildDeferred     bool
	}

	data := Data{ProjectName: projectName, SchemaName: schemaName, Schema: schema, OrReplace: orReplace, CreateIfNotExists: createIfNotExists, BuildDeferred: buildDeferred}

	for k, v := range schema.MvProperties {
		data.MvProperties = append(data.MvProperties, kvPair{k, v})
	}
	var out bytes.Buffer
	err = tpl.Execute(&out, data)
	if err != nil {
		panic(err)
	}
	return out.String(), nil
}

func (schema *TableSchema) ToSQLString(projectName string, schemaName string, createIfNotExists bool) (string, error) {
	baseSql, err := schema.ToBaseSQLString(projectName, schemaName, createIfNotExists, false)
	if err != nil {
		return "", errors.WithStack(err)
	}

	// 添加hash clustering或range clustering
	clusterInfo := schema.ClusterInfo
	if len(clusterInfo.ClusterCols) > 0 {

		if clusterInfo.ClusterType == CLUSTER_TYPE.Hash {
			baseSql += "\nclustered by (" + strings.Join(clusterInfo.ClusterCols, ", ") + ")"
		}

		if clusterInfo.ClusterType == CLUSTER_TYPE.Range {
			baseSql += "\nrange clustered by (" + strings.Join(clusterInfo.ClusterCols, ", ") + ")"
		}

		sortColsNum := len(clusterInfo.SortCols)
		if sortColsNum > 0 {
			baseSql += "\nsorted by ("

			for i, sc := range clusterInfo.SortCols {
				baseSql += sc.Name + " " + string(sc.Order)

				if i < sortColsNum-1 {
					baseSql += ", "
				}
			}

			baseSql += ")"
		}

		if clusterInfo.BucketNum > 0 {
			baseSql += "\nINTO " + strconv.Itoa(clusterInfo.BucketNum) + " BUCKETS"
		}
	}

	if schema.Lifecycle > 0 {
		baseSql += fmt.Sprintf("\nlifecycle %d", schema.Lifecycle)
	}

	baseSql += ";"

	return baseSql, nil
}

func (schema *TableSchema) ToExternalSQLString(
	projectName string,
	schemaName string,
	createIfNotExists bool,
	serdeProperties map[string]string,
	jars []string) (string, error) {

	if schema.StorageHandler == "" {
		return "", errors.New("TableSchema.StorageHandler is not set")
	}

	if schema.Location == "" {
		return "", errors.New("TableSchema.Location is not set")
	}

	baseSql, err := schema.ToBaseSQLString(projectName, schemaName, createIfNotExists, true)

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

func (schema *TableSchema) ToArrowSchema() *arrow.Schema {
	fields := make([]arrow.Field, len(schema.Columns))
	for i, column := range schema.Columns {
		arrowType, _ := TypeToArrowType(column.Type)
		fields[i] = arrow.Field{
			Name:     column.Name,
			Type:     arrowType,
			Nullable: column.IsNullable,
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
