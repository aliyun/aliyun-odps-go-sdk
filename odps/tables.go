package odps

import (
	"encoding/json"
	"encoding/xml"
	"fmt"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/common"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/tableschema"
	"github.com/pkg/errors"
	"net/url"
	"strings"
)

// Tables used for get all the tables in an odps project
type Tables struct {
	projectName string
	odpsIns     *Odps
}

// NewTables if projectName is not set，the default projectName of odps will be used
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

// List get all the tables, filters can be specified with TableFilter.NamePrefix,
// TableFilter.Extended, TableFilter.Owner
func (ts *Tables) List(filters ...TFilterFunc) ([]Table, error) {
	queryArgs := make(url.Values, 4)
	queryArgs.Set("expectmarker", "true")

	for _, filter := range filters {
		filter(queryArgs)
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
	var tables []Table

	for {
		err := client.GetWithModel(resource, queryArgs, &resModel)
		if err != nil {
			return tables, errors.WithStack(err)
		}

		if len(resModel.Tables) == 0 {
			break
		}

		for _, tableModel := range resModel.Tables {
			table := NewTable(ts.odpsIns, ts.projectName, tableModel.Name)
			table.model = tableModel

			tables = append(tables, table)
		}

		if resModel.Marker != "" {
			queryArgs.Set("marker", resModel.Marker)
			resModel = ResModel{}
		} else {
			break
		}
	}

	return tables, nil
}

// BatchLoadTables can get at most 100 tables, and the information of table is according to the permission
func (ts *Tables) BatchLoadTables(tableNames []string) ([]Table, error) {
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

	queryArgs := make(url.Values, 1)
	queryArgs.Set("query", "")
	rb := common.ResourceBuilder{ProjectName: ts.projectName}
	resource := rb.Tables()
	client := ts.odpsIns.restClient

	err := client.DoXmlWithModel(common.HttpMethod.PostMethod, resource, queryArgs, &postBodyModel, &resModel)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	ret := make([]Table, len(resModel.Table))

	for i, tableModel := range resModel.Table {
		table := NewTable(ts.odpsIns, ts.projectName, tableModel.Name)
		table.model = tableModel
		ret[i] = table
	}

	return ret, nil
}

// Create table with schema, the schema can be build with tableschema.SchemaBuilder
// parameter hints can affect the `Set` sql execution, like odps.mapred.map.split.size
// you can get introduce about alias from the reference of alias command
func (ts *Tables) Create(
	schema tableschema.TableSchema,
	createIfNotExists bool,
	hints, alias map[string]string) (*Instance, error) {

	sql, err := schema.ToSQLString(ts.projectName, createIfNotExists)
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

	instances := NewInstances(ts.odpsIns, ts.projectName)

	return instances.CreateTask(ts.projectName, &task)
}

func (ts *Tables) CreateAndWait(
	schema tableschema.TableSchema,
	createIfNotExists bool,
	hints, alias map[string]string) error {

	instance, err := ts.Create(schema, createIfNotExists, hints, alias)

	if err != nil {
		return errors.WithStack(err)
	}

	return errors.WithStack(instance.WaitForSuccess())
}

// CreateExternal create external table, the schema can be build with tableschema.SchemaBuilder
func (ts *Tables) CreateExternal(
	schema tableschema.TableSchema,
	createIfNotExists bool,
	serdeProperties map[string]string,
	jars []string,
	hints, alias map[string]string) (*Instance, error) {

	sql, err := schema.ToExternalSQLString(ts.projectName, createIfNotExists, serdeProperties, jars)
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

	instances := NewInstances(ts.odpsIns, ts.projectName)

	i, err := instances.CreateTask(ts.projectName, &task)
	return i, errors.WithStack(err)
}

func (ts *Tables) CreateExternalAndWait(
	schema tableschema.TableSchema,
	createIfNotExists bool,
	serdeProperties map[string]string,
	jars []string,
	hints, alias map[string]string) error {

	instance, err := ts.CreateExternal(schema, createIfNotExists, serdeProperties, jars, hints, alias)

	if err != nil {
		return errors.WithStack(err)
	}

	return errors.WithStack(instance.WaitForSuccess())
}

func (ts *Tables) CreateWithDataHub(
	schema tableschema.TableSchema,
	createIfNotExists bool,
	shardNum,
	hubLifecycle int,
) (*Instance, error) {

	sql, err := schema.ToBaseSQLString(ts.projectName, createIfNotExists, false)
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

	instances := NewInstances(ts.odpsIns, ts.projectName)

	i, err := instances.CreateTask(ts.projectName, &task)
	return i, errors.WithStack(err)
}

func (ts *Tables) CreateWithDataHubAndWait(
	schema tableschema.TableSchema,
	createIfNotExists bool,
	shardNum,
	hubLifecycle int,
) error {
	instance, err := ts.CreateWithDataHub(schema, createIfNotExists, shardNum, hubLifecycle)

	if err != nil {
		return errors.WithStack(err)
	}

	return errors.WithStack(instance.WaitForSuccess())
}

// Delete delete table
func (ts *Tables) Delete(tableName string, ifExists bool) (*Instance, error) {
	var sqlBuilder strings.Builder
	sqlBuilder.WriteString("drop table")
	if ifExists {
		sqlBuilder.WriteString(" if exists")
	}

	sqlBuilder.WriteRune(' ')
	sqlBuilder.WriteString(ts.projectName)
	sqlBuilder.WriteRune('.')
	sqlBuilder.WriteString(tableName)
	sqlBuilder.WriteString(";")

	sqlTask := NewSqlTask("SQLDropTableTask", sqlBuilder.String(), "", nil)
	instances := NewInstances(ts.odpsIns, ts.projectName)
	i, err := instances.CreateTask(ts.projectName, &sqlTask)
	return i, errors.WithStack(err)
}

func (ts *Tables) DeleteAndWait(tableName string, ifExists bool) error {
	instance, err := ts.Delete(tableName, ifExists)
	if err != nil {
		return errors.WithStack(err)
	}

	return errors.WithStack(instance.WaitForSuccess())
}

type TFilterFunc func(url.Values)

var TableFilter = struct {
	// Weather get extended information or not
	Extended func() TFilterFunc
	// Filter out tables with name prefix
	NamePrefix func(string) TFilterFunc
	// Filter out tables with owner name
	Owner func(string) TFilterFunc
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
}
