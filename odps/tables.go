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
	"fmt"
	"net/url"
	"strings"

	"github.com/aliyun/aliyun-odps-go-sdk/odps/common"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/tableschema"
	"github.com/pkg/errors"
)

// Tables used for get all the tables in an odps project
type Tables struct {
	projectName string
	schemaName  string
	odpsIns     *Odps
}

// NewTables if projectName is not set，the default projectName of odps will be used
func NewTables(odpsIns *Odps, projectName, schemaName string) *Tables {
	if projectName == "" {
		projectName = odpsIns.DefaultProjectName()
	}
	if schemaName == "" {
		schemaName = odpsIns.CurrentSchemaName()
	}
	return &Tables{
		projectName: projectName,
		schemaName:  schemaName,
		odpsIns:     odpsIns,
	}
}

// List get all the tables, filters can be specified with TableFilter.NamePrefix,
// TableFilter.Extended, TableFilter.Owner
func (ts *Tables) List(f func(*Table, error), filters ...TFilterFunc) {
	queryArgs := make(url.Values, 4)
	queryArgs.Set("expectmarker", "true")
	queryArgs.Set("curr_schema", ts.schemaName)

	for _, filter := range filters {
		if filter != nil {
			filter(queryArgs)
		}
	}

	rb := common.ResourceBuilder{ProjectName: ts.projectName}
	resource := rb.Tables()
	client := ts.odpsIns.restClient

	type ResModel struct {
		XMLName  xml.Name     `xml:"Tables"`
		Tables   []tableModel `xml:"Table"`
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

		for _, tableModel := range resModel.Tables {
			table := NewTable(ts.odpsIns, ts.projectName, ts.schemaName, tableModel.Name)
			table.model = tableModel

			f(table, nil)
		}

		if resModel.Marker != "" {
			queryArgs.Set("marker", resModel.Marker)
			resModel = ResModel{}
		} else {
			break
		}
	}
}

// BatchLoadTables can get at most 100 tables, and the information of table is according to the permission
func (ts *Tables) BatchLoadTables(tableNames []string) ([]*Table, error) {
	type PostBodyModel struct {
		XMLName xml.Name `xml:"Tables"`
		Tables  []struct {
			Project string
			Name    string
		} `xml:"Table"`
	}

	var postBodyModel PostBodyModel
	for _, tableName := range tableNames {
		postBodyModel.Tables = append(postBodyModel.Tables, struct {
			Project string
			Name    string
		}{Project: ts.projectName, Name: tableName})
	}

	type ResModel struct {
		XMLName xml.Name `xml:"Tables"`
		Table   []tableModel
	}

	var resModel ResModel

	queryArgs := make(url.Values, 4)
	queryArgs.Set("query", "")
	if ts.schemaName != "" {
		queryArgs.Set("curr_schema", ts.schemaName)
	}
	rb := common.ResourceBuilder{ProjectName: ts.projectName}
	resource := rb.Tables()
	client := ts.odpsIns.restClient

	err := client.DoXmlWithModel(common.HttpMethod.PostMethod, resource, queryArgs, &postBodyModel, &resModel)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	ret := make([]*Table, len(resModel.Table))

	for i, tableModel := range resModel.Table {
		table := NewTable(ts.odpsIns, ts.projectName, ts.schemaName, tableModel.Name)
		table.model = tableModel
		ret[i] = table
	}

	return ret, nil
}

func (ts *Tables) Get(tableName string) *Table {
	table := NewTable(ts.odpsIns, ts.projectName, ts.schemaName, tableName)
	return table
}

// Create table with schema, the schema can be build with tableschema.SchemaBuilder
// parameter hints can affect the `Set` sql execution, like odps.mapred.map.split.size
// you can get introduce about alias from the reference of alias command
func (ts *Tables) Create(
	schema tableschema.TableSchema,
	createIfNotExists bool,
	hints, alias map[string]string) error {

	sql, err := schema.ToSQLString(ts.projectName, ts.schemaName, createIfNotExists)
	if err != nil {
		return errors.WithStack(err)
	}
	if hints == nil {
		hints = make(map[string]string)
	}
	if ts.schemaName == "" {
		hints["odps.namespace.schema"] = "false"
	} else {
		hints["odps.namespace.schema"] = "true"
	}

	task := NewSqlTask("SQLCreateTableTask", sql, hints)

	// TODO rm aliases
	if alias != nil {
		aliasJson, _ := json.Marshal(alias)
		task.AddProperty("aliases", string(aliasJson))
	}

	instances := NewInstances(ts.odpsIns, ts.projectName)

	ins, err := instances.CreateTask(ts.projectName, &task)
	if err != nil {
		return errors.WithStack(err)
	}

	return errors.WithStack(ins.WaitForSuccess())
}

// CreateExternal create external table, the schema can be build with tableschema.SchemaBuilder
func (ts *Tables) CreateExternal(
	schema tableschema.TableSchema,
	createIfNotExists bool,
	serdeProperties map[string]string,
	jars []string,
	hints, alias map[string]string) error {

	sql, err := schema.ToExternalSQLString(ts.projectName, ts.schemaName, createIfNotExists, serdeProperties, jars)
	if err != nil {
		return errors.WithStack(err)
	}

	if hints == nil {
		hints = make(map[string]string)
	}
	if ts.schemaName == "" {
		hints["odps.namespace.schema"] = "false"
	} else {
		hints["odps.namespace.schema"] = "true"
	}
	task := NewSqlTask("SQLCreateExternalTableTask", sql, hints)

	if alias != nil {
		aliasJson, _ := json.Marshal(hints)
		task.AddProperty("aliases", string(aliasJson))
	}

	instances := NewInstances(ts.odpsIns, ts.projectName)

	i, err := instances.CreateTask(ts.projectName, &task)
	if err != nil {
		return errors.WithStack(err)
	}
	return errors.WithStack(i.WaitForSuccess())
}

func (ts *Tables) CreateWithDataHub(
	schema tableschema.TableSchema,
	createIfNotExists bool,
	shardNum,
	hubLifecycle int,
) error {

	sql, err := schema.ToBaseSQLString(ts.projectName, ts.schemaName, createIfNotExists, false)
	if err != nil {
		return errors.WithStack(err)
	}

	var sb strings.Builder
	sb.WriteString(sql)

	if schema.Lifecycle > 0 {
		sb.WriteString(fmt.Sprintf("\nlifecycle %d", schema.Lifecycle))
	}

	sb.WriteString(fmt.Sprintf("\ninto %d shards", shardNum))
	sb.WriteString(fmt.Sprintf("\nhubLifecycle %d", hubLifecycle))
	sb.WriteRune(';')

	hints := make(map[string]string)
	if ts.schemaName == "" {
		hints["odps.namespace.schema"] = "false"
	} else {
		hints["odps.namespace.schema"] = "true"
	}
	task := NewSqlTask("SQLCreateTableTaskWithDataHub", sb.String(), hints)

	instances := NewInstances(ts.odpsIns, ts.projectName)
	i, err := instances.CreateTask(ts.projectName, &task)

	if err != nil {
		return errors.WithStack(err)
	}

	return errors.WithStack(i.WaitForSuccess())
}

// Delete delete table
func (ts *Tables) Delete(tableName string, ifExists bool) error {
	var sqlBuilder strings.Builder
	hints := make(map[string]string)
	hints["odps.namespace.schema"] = "false"
	sqlBuilder.WriteString("drop table")
	if ifExists {
		sqlBuilder.WriteString(" if exists")
	}

	sqlBuilder.WriteRune(' ')
	sqlBuilder.WriteString(ts.projectName)
	sqlBuilder.WriteRune('.')
	if ts.schemaName != "" {
		hints["odps.namespace.schema"] = "true"
		sqlBuilder.WriteString("`" + ts.schemaName + "`")
		sqlBuilder.WriteRune('.')
	}
	sqlBuilder.WriteString("`" + tableName + "`")
	sqlBuilder.WriteString(";")

	sqlTask := NewSqlTask("SQLDropTableTask", sqlBuilder.String(), hints)
	instances := NewInstances(ts.odpsIns, ts.projectName)
	i, err := instances.CreateTask(ts.projectName, &sqlTask)
	if err != nil {
		return errors.WithStack(err)
	}
	return errors.WithStack(i.WaitForSuccess())
}

type TFilterFunc func(url.Values)

var TableFilter = struct {
	// Weather get extended information or not
	Extended func() TFilterFunc
	// Filter out tables with name prefix
	NamePrefix func(string) TFilterFunc
	// Filter out tables with owner name
	Owner func(string) TFilterFunc
	// Filter out tables with table type
	Type func(TableType) TFilterFunc
}{
	Extended: func() TFilterFunc {
		return func(values url.Values) {
			values.Set("extended", "")
		}
	},
	NamePrefix: func(name string) TFilterFunc {
		return func(values url.Values) {
			values.Set("name", name)
		}
	},
	Owner: func(owner string) TFilterFunc {
		return func(values url.Values) {
			values.Set("owner", owner)
		}
	},
	Type: func(tableType TableType) TFilterFunc {
		return func(values url.Values) {
			values.Set("type", tableType.String())
		}
	},
}

// TableCreator is used to create table
type TableCreator struct {
	odps             *Odps
	tableId          *TableIdentifier
	dataColumn       []tableschema.Column
	partitionColumn  []tableschema.Column
	comment          string
	ifNotExists      bool
	lifeCycle        *int64
	hints            map[string]string
	aliases          map[string]string
	primaryKeys      []string
	debug            bool
	shardNum         *int64
	hubLifecycle     *int64
	transactionTable bool
	tblProperties    map[string]string
	storedBy         string
	location         string
	jars             []string
	serdeProperties  map[string]string
}

func NewTableCreator(odps *Odps, identifier *TableIdentifier, dataColumn []tableschema.Column) *TableCreator {
	projectName := identifier.ProjectName
	if projectName == "" {
		identifier.ProjectName = odps.DefaultProjectName()
	}

	schemaName := identifier.SchemaName
	if schemaName == "" {
		identifier.SchemaName = odps.CurrentSchemaName()
	}

	return &TableCreator{
		odps:            odps,
		tableId:         identifier,
		dataColumn:      dataColumn,
		hints:           make(map[string]string),
		aliases:         make(map[string]string),
		tblProperties:   make(map[string]string),
		serdeProperties: make(map[string]string),
	}
}

func (tc *TableCreator) PartitionColumns(columns []tableschema.Column) *TableCreator {
	tc.partitionColumn = columns
	return tc
}

func (tc *TableCreator) WithComment(comment string) *TableCreator {
	tc.comment = comment
	return tc
}

func (tc *TableCreator) IfNotExists() *TableCreator {
	tc.ifNotExists = true
	return tc
}

func (tc *TableCreator) WithLifeCycle(lifeCycle int64) *TableCreator {
	tc.lifeCycle = &lifeCycle
	return tc
}

func (tc *TableCreator) TransactionTable() *TableCreator {
	if tc.tblProperties == nil {
		tc.tblProperties = make(map[string]string)
	}
	tc.tblProperties["transactional"] = "true"

	tc.hints["odps.sql.upsertable.table.enable"] = "true"
	tc.transactionTable = true
	return tc
}

func (tc *TableCreator) WithPrimaryKeys(primaryKeys []string) *TableCreator {
	tc.primaryKeys = primaryKeys
	return tc
}

func (tc *TableCreator) WithBucketNum(bucketNum int) *TableCreator {
	if bucketNum < 1 {
		panic("bucketNum must be greater than or equal to 1")
	}
	tc.tblProperties["write.bucket.num"] = fmt.Sprintf("%d", bucketNum)
	return tc
}

func (tc *TableCreator) WithTblProperties(tblProperties map[string]string) *TableCreator {
	for k, v := range tblProperties {
		tc.tblProperties[k] = v
	}
	return tc
}

func (tc *TableCreator) WithSerdeProperties(serdeProperties map[string]string) *TableCreator {
	for k, v := range serdeProperties {
		tc.serdeProperties[k] = v
	}
	return tc
}

func (tc *TableCreator) WithHints(hints map[string]string) *TableCreator {
	for k, v := range hints {
		tc.hints[k] = v
	}
	return tc
}

func (tc *TableCreator) WithAliases(aliases map[string]string) *TableCreator {
	for k, v := range aliases {
		tc.aliases[k] = v
	}
	return tc
}

func (tc *TableCreator) WithShardNum(shardNum int64) *TableCreator {
	tc.shardNum = &shardNum
	return tc
}

func (tc *TableCreator) WithHubLifecycle(hubLifecycle int64) *TableCreator {
	tc.hubLifecycle = &hubLifecycle
	return tc
}

func (tc *TableCreator) WithJars(jars []string) *TableCreator {
	tc.jars = append(tc.jars, jars...)
	return tc
}

func (tc *TableCreator) Debug() *TableCreator {
	tc.debug = true
	return tc
}

func (tc *TableCreator) Create() error {
	if tc.odps == nil {
		return errors.New("odps cannot be nil")
	}
	if tc.tableId == nil || tc.tableId.TableName == "" {
		return errors.New("identifier cannot be nil or empty")
	}
	if tc.dataColumn == nil || len(tc.dataColumn) == 0 {
		return errors.New("tableSchema cannot be nil")
	}

	// Set schema flag in hints
	if tc.tableId.SchemaName != "" {
		tc.hints["odps.namespace.schema"] = "true"
	} else {
		tc.hints["odps.namespace.schema"] = "false"
	}

	task := NewSqlTask("SQLCreateTableTask", tc.generateCreateTableSql(), tc.hints)

	if tc.aliases != nil && len(tc.aliases) > 0 {
		aliasJson, _ := json.Marshal(tc.aliases)
		task.AddProperty("aliases", string(aliasJson))
	}

	instances := NewInstances(tc.odps, tc.tableId.ProjectName)

	ins, err := instances.CreateTask(tc.tableId.ProjectName, &task)
	if err != nil {
		return errors.WithStack(err)
	}

	if tc.debug {
		logView, ignore := tc.odps.LogView().GenerateLogView(ins, 24)
		if ignore == nil {
			fmt.Println(logView)
		}
	}
	return errors.WithStack(ins.WaitForSuccess())
}

func (tc *TableCreator) CreateExternal(storedBy, location string) error {
	if tc.odps == nil {
		return errors.New("odps cannot be nil")
	}
	if tc.tableId == nil || tc.tableId.TableName == "" {
		return errors.New("identifier cannot be nil or empty")
	}
	if tc.dataColumn == nil || len(tc.dataColumn) == 0 {
		return errors.New("tableSchema cannot be nil")
	}

	tc.storedBy = storedBy
	tc.location = location

	if tc.tableId.SchemaName != "" {
		tc.hints["odps.namespace.schema"] = "true"
	} else {
		tc.hints["odps.namespace.schema"] = "false"
	}
	task := NewSqlTask("SQLCreateTableTask", tc.generateCreateExternalTableSql(), tc.hints)

	if tc.aliases != nil && len(tc.aliases) > 0 {
		aliasJson, _ := json.Marshal(tc.aliases)
		task.AddProperty("aliases", string(aliasJson))
	}

	instances := NewInstances(tc.odps, tc.tableId.ProjectName)

	ins, err := instances.CreateTask(tc.tableId.ProjectName, &task)
	if err != nil {
		return errors.WithStack(err)
	}

	if tc.debug {
		logView, ignore := tc.odps.LogView().GenerateLogView(ins, 24)
		if ignore == nil {
			fmt.Println(logView)
		}
	}
	return errors.WithStack(ins.WaitForSuccess())
}

func (tc *TableCreator) generateCreateExternalTableSql() string {
	if tc.storedBy == "" || tc.location == "" {
		panic("Create external table must have storedBy and location")
	}

	plainString := tc.generateCreateTableSql()
	plainString = strings.Replace(plainString, "CREATE", "CREATE EXTERNAL", 1)

	var sb strings.Builder
	sb.WriteString(" STORED BY '")
	sb.WriteString(strings.TrimSpace(tc.storedBy))
	sb.WriteString("'")

	if len(tc.serdeProperties) > 0 {
		sb.WriteString(" WITH SERDEPROPERTIES(")
		for k, v := range tc.serdeProperties {
			sb.WriteString(fmt.Sprintf("'%s' = '%s'", k, v))
		}
		sb.WriteString(")")
	}

	sb.WriteString(fmt.Sprintf(" LOCATION '%s'", tc.location))

	if len(tc.jars) > 0 {
		sb.WriteString(" USING '")
		sb.WriteString(strings.Join(tc.jars, ","))
		sb.WriteString("'")
	}

	if tc.lifeCycle != nil {
		// Remove LIFE CYCLE from original SQL query
		lastIndex := strings.LastIndex(plainString, " LIFECYCLE ")
		if lastIndex < 0 {
			panic("Invalid SQL syntax")
		}
		plainString = plainString[:lastIndex]
		sb.WriteString(fmt.Sprintf(" LIFECYCLE %d;", *tc.lifeCycle))
	} else {
		// Remove the trailing ';'
		plainString = strings.TrimSuffix(plainString, ";")
		sb.WriteString(";")
	}

	if tc.debug {
		fmt.Println(plainString + sb.String())
	}
	return plainString + sb.String()
}

func (tc *TableCreator) generateCreateTableSql() string {
	var sql strings.Builder
	sql.WriteString("CREATE TABLE ")
	if tc.ifNotExists {
		sql.WriteString("IF NOT EXISTS ")
	}
	sql.WriteString(tc.tableId.String())
	sql.WriteString(" (")

	columns := tc.dataColumn
	for i, column := range columns {
		sql.WriteString(fmt.Sprintf("`%s` %s", column.Name, column.Type.Name()))
		if !column.IsNullable {
			sql.WriteString(" NOT NULL")
		}
		if column.DefaultValue != "" {
			sql.WriteString(" DEFAULT " + column.DefaultValue)
		}
		if column.Comment != "" {
			sql.WriteString(fmt.Sprintf(" COMMENT '%s'", column.Comment))
		}
		if i+1 < len(columns) {
			sql.WriteString(",")
		}
	}

	if tc.transactionTable {
		if len(tc.primaryKeys) == 0 {
			panic("Transaction table must have a primary key")
		}
		sql.WriteString(", PRIMARY KEY(" + strings.Join(tc.primaryKeys, ", ") + ")")
	}
	sql.WriteString(")")

	if tc.comment != "" {
		sql.WriteString(fmt.Sprintf(" COMMENT '%s'", tc.comment))
	}

	partitionColumns := tc.partitionColumn
	if len(partitionColumns) > 0 {
		sql.WriteString(" PARTITIONED BY (")
		for i, column := range partitionColumns {
			sql.WriteString(fmt.Sprintf("%s %s", column.Name, column.Type.Name()))
			if column.Comment != "" {
				sql.WriteString(fmt.Sprintf(" COMMENT '%s'", column.Comment))
			}
			if i+1 < len(partitionColumns) {
				sql.WriteString(",")
			}
		}
		sql.WriteString(")")
	}

	if len(tc.tblProperties) > 0 {
		sql.WriteString(" TBLPROPERTIES(")

		first := true
		for k, v := range tc.tblProperties {
			if !first {
				sql.WriteString(", ")
			}
			sql.WriteString(fmt.Sprintf("'%s'='%s'", k, v))
			first = false
		}

		sql.WriteString(")")
	}

	if tc.lifeCycle != nil {
		sql.WriteString(fmt.Sprintf(" LIFECYCLE %d", *tc.lifeCycle))
	}
	if tc.shardNum != nil {
		sql.WriteString(fmt.Sprintf(" INTO %d SHARDS", *tc.shardNum))
	}
	if tc.hubLifecycle != nil {
		if tc.lifeCycle != nil {
			panic("Only one of lifeCycle and hubLifecycle can be set")
		}
		sql.WriteString(fmt.Sprintf(" HUBLIFECYCLE %d", *tc.hubLifecycle))
	}
	sql.WriteString(";")

	if tc.debug {
		fmt.Println(sql.String())
	}
	return sql.String()
}
