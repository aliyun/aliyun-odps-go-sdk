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
	"github.com/aliyun/aliyun-odps-go-sdk/odps/common"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/restclient"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/tableschema"
	"github.com/pkg/errors"
	"net/url"
	"strconv"
	"strings"
	"time"
)

type TableType int

const (
	ManagedTable TableType = iota
	VirtualView
	ExternalTable
	TableTypeUnknown
)

// Table represent the table in odps projects
type Table struct {
	model            tableModel
	tableSchema      tableschema.TableSchema
	odpsIns          *Odps
	beLoaded         bool
	beLoadedExtended bool
}

// TableOrErr is used for the return value of Tables.List
type TableOrErr struct {
	Table *Table
	Err   error
}

type tableModel struct {
	XMLName       xml.Name `xml:"Table"`
	Name          string
	TableId       string
	Format        string
	Schema        string
	Comment       string
	Owner         string
	ProjectName   string `xml:"Project"`
	SchemaName    string
	TableLabel    string
	CryptoAlgo    string
	TableMaskInfo string
	Type          TableType
}

func NewTable(odpsIns *Odps, projectName string, schemaName string, tableName string) *Table {
	return &Table{
		model:   tableModel{ProjectName: projectName, SchemaName: schemaName, Name: tableName},
		odpsIns: odpsIns,
	}
}

func newTableWithModel(odpsIns *Odps, model *tableModel) (*Table, error) {
	table := Table{
		model:            *model,
		odpsIns:          odpsIns,
		beLoaded:         true,
		beLoadedExtended: false,
	}

	err := json.Unmarshal([]byte(model.Schema), &table.tableSchema)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return &table, nil
}

func (t *Table) IsLoaded() bool {
	return t.beLoaded
}

func (t *Table) IsLoadedExtended() bool {
	return t.beLoaded
}

func (t *Table) Load() error {
	client := t.odpsIns.restClient
	resource := t.ResourceUrl()
	t.beLoaded = true

	queryArgs := make(url.Values, 4)
	if t.SchemaName() != "" {
		queryArgs.Set("curr_schema", t.SchemaName())
	}

	err := client.GetWithModel(resource, queryArgs, &t.model)
	if err != nil {
		return errors.WithStack(err)
	}

	err = json.Unmarshal([]byte(t.model.Schema), &t.tableSchema)
	if err != nil {
		return errors.WithStack(err)
	}

	// LoadExtendedInfo会加载其他额外信息，主要是table schema信息
	return errors.WithStack(t.LoadExtendedInfo())
}

func (t *Table) LoadExtendedInfo() error {
	client := t.odpsIns.restClient
	resource := t.ResourceUrl()

	queryArgs := make(url.Values, 4)
	queryArgs.Set("extended", "")
	if t.SchemaName() != "" {
		queryArgs.Set("curr_schema", t.SchemaName())
	}

	var model tableModel
	err := client.GetWithModel(resource, queryArgs, &model)
	if err != nil {
		return errors.WithStack(err)
	}

	// 填充剩余的schema信息
	err = json.Unmarshal([]byte(model.Schema), &t.tableSchema)
	if err != nil {
		return errors.WithStack(err)
	}

	// Reserved信息中提取出Cluster信息
	err = json.Unmarshal([]byte(t.tableSchema.Reserved), &t.tableSchema.ClusterInfo)
	if err != nil {
		return errors.WithStack(err)
	}
	t.beLoadedExtended = true
	return nil
}

func (t *Table) Name() string {
	return t.model.Name
}

func (t *Table) ResourceUrl() string {
	rb := common.ResourceBuilder{ProjectName: t.ProjectName()}
	return rb.Table(t.Name())
}

func (t *Table) Comment() string {
	// both model.Comment and tableSchema.Comment can get the value, the latter takes precedence
	if t.tableSchema.Comment != "" {
		return t.tableSchema.Comment
	}
	return t.model.Comment
}

func (t *Table) Owner() string {
	// both model.Owner and tableSchema.Owner can get the value, the latter takes precedence
	if t.tableSchema.Owner != "" {
		return t.tableSchema.Owner
	}
	return t.model.Owner
}

func (t *Table) TableID() string {
	return t.model.TableId
}

func (t *Table) CryptoAlgo() string {
	return t.model.CryptoAlgo
}

func (t *Table) ProjectName() string {
	return t.model.ProjectName
}

func (t *Table) SchemaName() string {
	return t.model.SchemaName
}

func (t *Table) Type() TableType {
	return t.model.Type
}

func (t *Table) CreatedTime() time.Time {
	return time.Time(t.tableSchema.CreateTime)
}

func (t *Table) TableLabel() string {
	// Service will return 0 if nothing set
	if t.tableSchema.TableLabel == "0" {
		return ""
	}
	return t.tableSchema.TableLabel
}

func (t *Table) TableExtendedLabels() []string {
	return t.tableSchema.ExtendedLabel
}

func (t *Table) MaxExtendedLabel() string {
	var labels []string
	extendedLabels := t.TableExtendedLabels()

	labels = append(labels, extendedLabels...)

	for _, column := range t.tableSchema.Columns {
		if column.Label != "" {
			labels = append(labels, column.Label)
		}
	}

	return calculateMaxLabel(labels)
}

// MaxLabel 获取最高的label级别
// Label的定义分两部分：
// 1. 业务分类：C，S，B
// 2. 数据等级：1，2，3，4
//
// 二者是正交关系，即C1,C2,C3,C4,S1,S2,S3,S4,B1,B2,B3,B4。
//
// MaxLabel的语意：
// 1. MaxLabel=max(TableLabel, ColumnLabel), max(...)函数的语意由Label中的数据等级决定：4>3>2>1
// 2. MaxLabel显示：
// 当最高等级Label只出现一次时，MaxLabel=业务分类+数据等级，例如：B4, C3，S2
// 当最高等级LabeL出现多次，但业务分类也唯一，MaxLabel=业务分类+数据等级，例如：B4, C3，S2
// 当最高等级Label出现多次，且业务不唯一，MaxLabel=L+数据等级，例如：L4， L3
func (t *Table) MaxLabel() string {
	var labels []string
	tableLabel := t.TableLabel()

	if tableLabel != "" {
		labels = append(labels, tableLabel)
	}

	for _, column := range t.tableSchema.Columns {
		if column.Label != "" {
			labels = append(labels, column.Label)
		}
	}

	return calculateMaxLabel(labels)
}

func calculateMaxLabel(labels []string) string {
	var maxLevel = 0
	var category string

	for _, label := range labels {
		if label == "" {
			continue
		}

		curCategory := label[0 : len(label)-1]
		num, err := strconv.Atoi(string(label[len(label)-1]))

		if err != nil {
			continue
		}

		if num > maxLevel {
			maxLevel = num
			category = ""
		}

		if curCategory == "" {
			category = "L"
			continue
		}

		curCategory = strings.ToLower(curCategory)
		if category == "" {
			category = curCategory
		} else if category != curCategory {
			category = "L"
		}
	}

	if category == "" && maxLevel == 0 {
		return ""
	}

	if category == "" {
		category = "L"
	}

	return fmt.Sprintf("%s%d", category, maxLevel)
}
func (t *Table) Exists() (bool, error) {
	err := t.Load()

	var httpErr restclient.HttpNotOk
	if errors.As(err, &httpErr) {
		if httpErr.Status == "404 Not Found" {
			return false, nil
		}

		return false, err
	}

	return true, nil
}

func (t *Table) LastDDLTime() time.Time {
	return time.Time(t.tableSchema.LastDDLTime)
}

func (t *Table) LastModifiedTime() time.Time {
	return time.Time(t.tableSchema.LastModifiedTime)
}

func (t *Table) isVirtualView() bool {
	return t.tableSchema.IsVirtualView
}

func (t *Table) isExternal() bool {
	return t.tableSchema.IsExternal
}

func (t *Table) ViewText() string {
	return t.tableSchema.ViewText
}

func (t *Table) Size() int64 {
	return t.tableSchema.Size
}

func (t *Table) RecordNum() int {
	return t.tableSchema.RecordNum
}

func (t *Table) LifeCycle() int {
	return t.tableSchema.Lifecycle
}

func (t *Table) HubLifeCycle() int {
	return t.tableSchema.HubLifecycle
}

func (t *Table) PartitionColumns() []tableschema.Column {
	return t.tableSchema.PartitionColumns
}

func (t *Table) ShardInfoJson() string {
	return t.tableSchema.ShardInfo
}

func (t *Table) Schema() tableschema.TableSchema {
	return t.tableSchema
}

func (t *Table) SchemaJson() string {
	return t.model.Schema
}

func (t *Table) Delete() error {
	var sqlBuilder strings.Builder
	sqlBuilder.WriteString("drop table")
	sqlBuilder.WriteString(" if exists")

	sqlBuilder.WriteRune(' ')
	sqlBuilder.WriteString(t.ProjectName())
	sqlBuilder.WriteRune('.')
	sqlBuilder.WriteString(t.Name())
	sqlBuilder.WriteString(";")

	sqlTask := NewSqlTask("SQLDropTableTask", sqlBuilder.String(), nil)
	instances := NewInstances(t.odpsIns, t.ProjectName())
	i, err := instances.CreateTask(t.ProjectName(), &sqlTask)
	if err != nil {
		return errors.WithStack(err)
	}
	return errors.WithStack(i.WaitForSuccess())
}

func (t *Table) ExecSqlWithHints(taskName, sql string, hints map[string]string) (*Instance, error) {
	task := NewSqlTask(taskName, sql, hints)
	instances := NewInstances(t.odpsIns, t.ProjectName())
	i, err := instances.CreateTask(t.ProjectName(), &task)
	return i, errors.WithStack(err)
}

func (t *Table) ExecSql(taskName, sql string) (*Instance, error) {
	return t.ExecSqlWithHints(taskName, sql, nil)
}

// 将格式为region=1026/name=abc格式的“分区值”转换为格式为region='1026',name='abc'的"分区spec"值
func partitionValueToSpec(value string) string {
	columns := strings.Split(value, "/")

	sb := strings.Builder{}
	for i, n := 0, len(columns); i < n; i++ {
		kv := strings.Split(columns[i], "=")

		sb.WriteString(fmt.Sprintf("%s='%s'", kv[0], kv[1]))

		if i < n-1 {
			sb.WriteString(",")
		}
	}

	return sb.String()
}

// AddPartitions Example: AddPartitions(true, []string{"region=10026/name=abc", "region=10027/name=mhn"})
func (t *Table) AddPartitions(ifNotExists bool, partitionValues []string) error {
	var sb strings.Builder
	sb.WriteString("alter table ")
	hints := make(map[string]string)
	if t.SchemaName() == "" {
		sb.WriteString(fmt.Sprintf("%s.%s", t.ProjectName(), t.Name()))
		hints["odps.namespace.schema"] = "false"
	} else {
		sb.WriteString(fmt.Sprintf("%s.%s.%s", t.ProjectName(), t.SchemaName(), t.Name()))
		hints["odps.namespace.schema"] = "true"
	}
	sb.WriteString(" add")
	if ifNotExists {
		sb.WriteString(" if not exists\n")
	}

	for _, partitionValue := range partitionValues {
		sb.WriteString("partition (")
		sb.WriteString(partitionValueToSpec(partitionValue))
		sb.WriteString(")\n")
	}

	sb.WriteString(";")
	println(sb.String())

	i, err := t.ExecSqlWithHints("SQLAddPartitionTask", sb.String(), hints)
	if err != nil {
		return errors.WithStack(err)
	}

	return errors.WithStack(i.WaitForSuccess())
}

// AddPartition Example: AddPartition(true, "region=10026/name=abc")
func (t *Table) AddPartition(ifNotExists bool, partitionValue string) error {
	return t.AddPartitions(ifNotExists, []string{partitionValue})
}

// DeletePartitions Example: DeletePartitions(true, []string{"region=10026/name=abc", "region=10027/name=mhn"})
func (t *Table) DeletePartitions(ifExists bool, partitionValues []string) error {
	var sb strings.Builder
	sb.WriteString("alter table ")
	hints := make(map[string]string)
	if t.SchemaName() == "" {
		hints["odps.namespace.schema"] = "false"
		sb.WriteString(fmt.Sprintf("%s.%s", t.ProjectName(), t.Name()))
	} else {
		hints["odps.namespace.schema"] = "true"
		sb.WriteString(fmt.Sprintf("%s.%s.%s", t.ProjectName(), t.SchemaName(), t.Name()))
	}
	sb.WriteString(" drop")
	if ifExists {
		sb.WriteString(" if exists")
	}

	n := len(partitionValues)
	for i, partitionValue := range partitionValues {
		sb.WriteString(" partition (")
		sb.WriteString(partitionValueToSpec(partitionValue))
		sb.WriteString(")")

		if i < n-1 {
			sb.WriteString(",\n")
		}
	}

	sb.WriteString(";")

	ins, err := t.ExecSqlWithHints("SQLDropPartitionTask", sb.String(), hints)
	if err != nil {
		return errors.WithStack(err)
	}

	err = ins.WaitForSuccess()
	if err != nil {
		return errors.WithStack(err)
	}

	return nil
}

// DeletePartition Example: DeletePartition(true, "region=10026/name=abc")
func (t *Table) DeletePartition(ifExists bool, partitionValue string) error {
	return t.DeletePartitions(ifExists, []string{partitionValue})
}

func (t *Table) GetPartitionValues() ([]string, error) {
	queryArgs := make(url.Values, 4)
	queryArgs.Set("partitions", "")
	// 指定name为空后，接口只会返回分区的“名字”，也就是分区的值q
	queryArgs.Set("name", "")

	if t.SchemaName() != "" {
		queryArgs.Set("curr_schema", t.SchemaName())
	}

	resource := t.ResourceUrl()
	client := t.odpsIns.restClient

	type ResModel struct {
		XMLName    xml.Name `xml:"Partitions"`
		Marker     string
		MaxItems   string
		Partitions []struct {
			Name string
		} `xml:"Partition"`
	}

	var resModel ResModel
	err := client.GetWithModel(resource, queryArgs, &resModel)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	partitionValues := make([]string, len(resModel.Partitions))
	for i, p := range resModel.Partitions {
		partitionValues[i] = p.Name
	}

	return partitionValues, err
}

// GetPartition get partitions with partitionKey like "region=10026/name=abc", error if not found
func (t *Table) GetPartition(partitionValue string) (Partition, error) {
	// 将region=10026/name=abc格式的分区值转换为region='10026',name='abc'
	columns := strings.Split(partitionValue, "/")

	sb := strings.Builder{}
	for i, n := 0, len(columns); i < n; i++ {
		kv := strings.Split(columns[i], "=")

		sb.WriteString(fmt.Sprintf("%s='%s'", kv[0], kv[1]))

		if i < n-1 {
			sb.WriteString(",")
		}
	}

	partitionSpec := sb.String()
	partitions, err := t.getPartitions(partitionSpec)
	if err != nil {
		return Partition{}, err
	}

	if len(partitions) == 0 {
		return Partition{}, errors.Errorf("Partition(%s) is not found", partitionValue)
	}

	return partitions[0], nil
}

// GetPartitions get partitions
func (t *Table) GetPartitions() ([]Partition, error) {
	return t.getPartitions("")
}

// partitionSpec格式为: region='10026',name='abc'
func (t *Table) getPartitions(partitionSpec string) ([]Partition, error) {
	queryArgs := make(url.Values, 4)
	queryArgs.Set("partitions", "")
	queryArgs.Set("expectmarker", "true")
	queryArgs.Set("maxitems", "10000")

	if partitionSpec != "" {
		queryArgs.Set("partition", partitionSpec)
	}
	if t.SchemaName() != "" {
		queryArgs.Set("curr_schema", t.SchemaName())
	}

	resource := t.ResourceUrl()
	client := t.odpsIns.restClient

	type ResModel struct {
		XMLName    xml.Name `xml:"Partitions"`
		Marker     string
		MaxItems   string
		Partitions []struct {
			Columns []struct {
				Name  string `xml:"Name,attr"`
				Value string `xml:"Value,attr"`
			} `xml:"Column"`
			CreationTime         int64
			LastDDLTime          int64
			LastModifiedTime     int64
			PartitionSize        int
			PartitionRecordCount int
		} `xml:"Partition"`
	}

	var resModel ResModel
	var partitions []Partition

	for {
		err := client.GetWithModel(resource, queryArgs, &resModel)
		if err != nil {
			return partitions, errors.WithStack(err)
		}

		var pModel partitionModel

		for _, p := range resModel.Partitions {
			pModel.Value = make([]PartitionColumn, len(p.Columns))
			for i, c := range p.Columns {
				pModel.Value[i] = PartitionColumn{
					Name:  c.Name,
					Value: c.Value,
				}
			}

			pModel.CreateTime = common.GMTTime(time.Unix(p.CreationTime, 0))
			pModel.LastDDLTime = common.GMTTime(time.Unix(p.LastDDLTime, 0))
			pModel.LastModifiedTime = common.GMTTime(time.Unix(p.LastModifiedTime, 0))
			pModel.PartitionSize = p.PartitionSize
			pModel.PartitionRecordNum = p.PartitionRecordCount

			partition := Partition{
				odpsIns:     t.odpsIns,
				projectName: t.ProjectName(),
				tableName:   t.Name(),
				model:       pModel,
			}

			partitions = append(partitions, partition)
		}

		if resModel.Marker != "" {
			queryArgs.Set("marker", resModel.Marker)
			resModel = ResModel{}
		} else {
			break
		}
	}

	return partitions, nil
}

// Read can get at most 1W records or 10M bytes of records. partition is a string like "region='10026, name='abc'"
// columns are the columns wanted. limit is the most records to get. timezone is the timezone of datetime type data,
// it can be ""
//func (t *Table) Read(partition string, columns []string, limit int, timezone string) (*csv.Reader, error) {
//	queryArgs := make(url.Values, 4)
//
//	queryArgs.Set("data", "")
//	if partition != "" {
//		queryArgs.Set("partition", partition)
//	}
//
//	if len(columns) > 0 {
//		queryArgs.Set("cols", strings.Join(columns, ","))
//	}
//
//	if limit > 0 {
//		queryArgs.Set("linenum", strconv.Itoa(limit))
//	}
//
//	client := t.odpsIns.restClient
//	resource := t.ResourceUrl()
//
//	req, err := client.NewRequestWithUrlQuery(common.HttpMethod.GetMethod, resource, nil, queryArgs)
//	if err != nil {
//		return nil, errors.WithStack(err)
//	}
//
//	if timezone != "" {
//		req.Header.Set(common.HttpHeaderSqlTimezone, timezone)
//	}
//
//	res, err := client.Do(req)
//	if err != nil {
//		return nil, errors.WithStack(err)
//	}
//
//	return csv.NewReader(res.Body), nil
//}

func (t *Table) CreateShards(shardCount int) error {
	var sb strings.Builder
	sb.WriteString("alter table ")
	hints := make(map[string]string)
	if t.SchemaName() == "" {
		sb.WriteString(fmt.Sprintf("%s.%s", t.ProjectName(), t.Name()))
		hints["odps.namespace.schema"] = "false"
	} else {
		sb.WriteString(fmt.Sprintf("%s.%s.%s", t.ProjectName(), t.SchemaName(), t.Name()))
		hints["odps.namespace.schema"] = "true"
	}
	sb.WriteString(fmt.Sprintf("\ninto %d shards;", shardCount))
	ins, err := t.ExecSqlWithHints("SQLCreateShardsTask", sb.String(), hints)
	if err != nil {
		return errors.WithStack(err)
	}

	err = ins.WaitForSuccess()
	if err != nil {
		return errors.WithStack(err)
	}

	return nil
}

func TableTypeFromStr(s string) TableType {
	switch s {
	case "MANAGED_TABLE":
		return ManagedTable
	case "VIRTUAL_VIEW":
		return VirtualView
	case "EXTERNAL_TABLE":
		return ExternalTable
	default:
		return TableTypeUnknown
	}
}

func (t TableType) String() string {
	switch t {
	case ManagedTable:
		return "MANAGED_TABLE"
	case VirtualView:
		return "VIRTUAL_VIEW"
	case ExternalTable:
		return "EXTERNAL_TABLE"
	default:
		return "Unknown"
	}
}

func (t *TableType) UnmarshalXML(d *xml.Decoder, start xml.StartElement) error {
	var s string

	if err := d.DecodeElement(&s, &start); err != nil {
		return errors.WithStack(err)
	}

	*t = TableTypeFromStr(s)

	return nil
}

func (t TableType) MarshalXML(d *xml.Encoder, start xml.StartElement) error {
	s := t.String()
	return errors.WithStack(d.EncodeElement(s, start))
}
