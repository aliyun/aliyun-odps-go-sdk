package odps

import (
	"encoding/json"
	"encoding/xml"
	"fmt"
	"github.com/pkg/errors"
	"net/url"
	"strings"
)

// Tables 表示ODPS中所有Table的集合
type Tables struct {
	projectName string
	odpsIns     *Odps
}

// NewTables 如果projectName没有指定，则使用Odps的默认项目名
func NewTables(odpsIns *Odps, projectName ...string) Tables {
	var _projectName string

	if projectName == nil {
		_projectName = odpsIns.DefaultProjectName()
	} else {
		_projectName = projectName[0]
	}

	return Tables{
		projectName: _projectName,
		odpsIns:     odpsIns,
	}
}

func (tables *Tables) List(c chan Table, extended bool, name, owner string) error {
	defer close(c)

	queryArgs := make(url.Values, 5)
	queryArgs.Set("expectmarker", "true")

	if extended {
		queryArgs.Set("extended", "")
	}

	if name != "" {
		queryArgs.Set("name", name)
	}

	if owner != "" {
		queryArgs.Set("owner", owner)
	}

	rb := ResourceBuilder{projectName: tables.projectName}
	resource := rb.Tables()
	client := tables.odpsIns.restClient

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
			return errors.WithStack(err)
		}

		if len(resModel.Tables) == 0 {
			break
		}

		for _, tableModel := range resModel.Tables {
			table := NewTable(tables.odpsIns, tables.projectName, tableModel.Name)
			table.model = tableModel

			c <- table
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

// BatchLoadTables 批量加载表信息, rest api 对请求数量有限制, 目前一次操作最多可请求 100 张表信息; 返回的表数据,与操作权限有关.
func (tables *Tables) BatchLoadTables(tableNames []string) ([]Table, error) {
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
		}{Project: tables.projectName, Name: tableName})
	}

	type ResModel struct {
		XMLName xml.Name `xml:"Tables"`
		Table   []tableModel
	}

	var resModel ResModel

	queryArgs := make(url.Values, 1)
	queryArgs.Set("query", "")
	rb := ResourceBuilder{projectName: tables.projectName}
	resource := rb.Tables()
	client := tables.odpsIns.restClient

	err := client.DoXmlWithModel(HttpMethod.PostMethod, resource, queryArgs, &postBodyModel, &resModel)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	ret := make([]Table, len(resModel.Table))

	for i, tableModel := range resModel.Table {
		table := NewTable(tables.odpsIns, tables.projectName, tableModel.Name)
		table.model = tableModel
		ret[i] = table
	}

	return ret, nil
}

// Create 创建表, 表的内容在schema中， 需要提前构建schema
func (tables *Tables) Create(
	schema TableSchema,
	createIfNotExists bool,
	hints, alias map[string]string) (*Instance, error) {

	sql, err := schema.ToSQLString(tables.projectName, createIfNotExists)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	task := NewSqlTask("SQLCreateTableTask", sql, "", nil)

	if hints != nil {
		hintsJson, _ := json.Marshal(hints)
		task.AddProperty("settings", string(hintsJson))
	}

	if alias != nil {
		aliasJson, _ := json.Marshal(hints)
		task.AddProperty("settings", string(aliasJson))
	}

	instances := NewInstances(tables.odpsIns, tables.projectName)

	return instances.CreateTask(tables.projectName, &task)
}

func (tables *Tables) CreateAndWait(
	schema TableSchema,
	createIfNotExists bool,
	hints, alias map[string]string) error {

	instance, err := tables.Create(schema, createIfNotExists, hints, alias)

	if err != nil {
		return errors.WithStack(err)
	}

	return errors.WithStack(instance.WaitForSuccess())
}

// CreateExternal 创建外部表, 表的内容在schema中， 需要提前构建schema
func (tables *Tables) CreateExternal(
	schema TableSchema,
	createIfNotExists bool,
	serdeProperties map[string]string,
	jars []string,
	hints, alias map[string]string) (*Instance, error) {

	sql, err := schema.ToExternalSQLString(tables.projectName, createIfNotExists, serdeProperties, jars)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	task := NewSqlTask("SQLCreateExternalTableTask", sql, "", nil)

	if hints != nil {
		hintsJson, _ := json.Marshal(hints)
		task.AddProperty("settings", string(hintsJson))
	}

	if alias != nil {
		aliasJson, _ := json.Marshal(hints)
		task.AddProperty("settings", string(aliasJson))
	}

	instances := NewInstances(tables.odpsIns, tables.projectName)

	i, err := instances.CreateTask(tables.projectName, &task)
	return i, errors.WithStack(err)
}

func (tables *Tables) CreateExternalAndWait(
	schema TableSchema,
	createIfNotExists bool,
	serdeProperties map[string]string,
	jars []string,
	hints, alias map[string]string) error {

	instance, err := tables.CreateExternal(schema, createIfNotExists, serdeProperties, jars, hints, alias)

	if err != nil {
		return errors.WithStack(err)
	}

	return errors.WithStack(instance.WaitForSuccess())
}

func (tables *Tables) CreateWithDataHub(
	schema TableSchema,
	createIfNotExists bool,
	shardNum,
	hubLifecycle int,
) (*Instance, error) {

	sql, err := schema.toSQLString(tables.projectName, createIfNotExists, false)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	var sb strings.Builder
	sb.WriteString(sql)

	if schema.Lifecycle > 0 {
		sb.WriteString(fmt.Sprintf("\nlifecycle %d", schema.Lifecycle))
	}

	sb.WriteString(fmt.Sprintf("\ninto %d shards", shardNum))
	sb.WriteString(fmt.Sprintf("\nhubLifecycle %d", hubLifecycle))
	sb.WriteRune(';')

	task := NewSqlTask("SQLCreateTableTaskWithDataHub", sb.String(), "", nil)

	instances := NewInstances(tables.odpsIns, tables.projectName)

	i, err := instances.CreateTask(tables.projectName, &task)
	return i, errors.WithStack(err)
}

func (tables *Tables) CreateWithDataHubAndWait(
	schema TableSchema,
	createIfNotExists bool,
	shardNum,
	hubLifecycle int,
) error {
	instance, err := tables.CreateWithDataHub(schema, createIfNotExists, shardNum, hubLifecycle)

	if err != nil {
		return errors.WithStack(err)
	}

	return errors.WithStack(instance.WaitForSuccess())
}

// Delete 删除表
func (tables *Tables) Delete(tableName string, ifExists bool) (*Instance, error) {
	var sqlBuilder strings.Builder
	sqlBuilder.WriteString("drop table")
	if ifExists {
		sqlBuilder.WriteString(" if exists")
	}

	sqlBuilder.WriteRune(' ')
	sqlBuilder.WriteString(tables.projectName)
	sqlBuilder.WriteRune('.')
	sqlBuilder.WriteString(tableName)
	sqlBuilder.WriteString(";")

	sqlTask := NewSqlTask("SQLDropTableTask", sqlBuilder.String(), "", nil)
	instances := NewInstances(tables.odpsIns, tables.projectName)
	i, err := instances.CreateTask(tables.projectName, &sqlTask)
	return i, errors.WithStack(err)
}

func (tables *Tables) DeleteAndWait(tableName string, ifExists bool) error {
	instance, err := tables.Delete(tableName, ifExists)
	if err != nil {
		return errors.WithStack(err)
	}

	return errors.WithStack(instance.WaitForSuccess())
}
