package odps

import (
	"encoding/json"
	"encoding/xml"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"
)

type TableType int

const (
	_ TableType = iota
	ManagedTable
	VirtualView
	ExternalTable
	TableTypeUnknown
)

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
		return "TableTypeUnknown"
	}
}

// Table 表示ODPS中的表
type Table struct {
	model       tableModel
	tableSchema TableSchema
	odpsIns     *Odps
	beLoaded    bool
}

type tableModel struct {
	XMLName     xml.Name `xml:"Table"`
	Name        string
	TableId     string
	Format      string
	Schema      string
	Comment     string
	Owner       string
	ProjectName string `xml:"Project"`
	TableLabel  string
	CryptoAlgo  string
	Type        TableType
}

func NewTable(odpsIns *Odps, projectName string, tableName string) Table {
	return Table{
		model:   tableModel{ProjectName: projectName, Name: tableName},
		odpsIns: odpsIns,
	}
}

func (t *Table) HasBeLoad() bool {
	return t.beLoaded
}

func (t *Table) Load() error {
	client := t.odpsIns.restClient
	rb := t.odpsIns.rb
	resource := rb.Table(t.model.Name)

	t.beLoaded = true

	err := client.GetWithModel(resource, nil, &t.model)
	if err != nil {
		return err
	}

	return json.Unmarshal([]byte(t.model.Schema), &t.tableSchema)
}

func (t *Table) LoadExtendedInfo() error {
	client := t.odpsIns.restClient
	rb := t.odpsIns.rb
	resource := rb.Table(t.model.Name)

	urlQuery := make(url.Values)
	urlQuery.Set("extended", "")

	req, err := client.NewRequestWithUrlQuery(GetMethod, resource, nil, urlQuery)
	if err != nil {
		return err
	}

	var model tableModel

	err = client.DoWithModel(req, &model)
	if err != nil {
		return err
	}

	return json.Unmarshal([]byte(model.Schema), &t.tableSchema)
}

func (t *Table) Name() string {
	return t.model.Name
}

func (t *Table) Comment() string {
	return t.model.Comment
}

func (t *Table) Owner() string {
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

func (t *Table) Type() TableType {
	return t.model.Type
}

func (t *Table) CreatedTime() time.Time {
	return time.Time(t.tableSchema.CreateTime)
}

func (t *Table) TableLabel() string {
	return t.tableSchema.TableLabel
}

func (t *Table) TableExtendedLabels() []string {
	return t.tableSchema.ExtendedLabel
}

func (t *Table) MaxExtendedLabel() string {
	var labels []string
	extendedLabels := t.TableExtendedLabels()

	for _, label := range extendedLabels {
		labels = append(labels, label)
	}

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

func (t *Table) Size() int {
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

func (t *Table) GetSchema() (*TableSchema, error)  {
	err := json.Unmarshal([]byte(t.model.Schema), &t.tableSchema)
	if err != nil  {
		return nil, err
	}

	return &t.tableSchema, nil
}

func (t *Table) Schema() TableSchema {
	return t.tableSchema
}

func (t *Table) SchemaJson() string {
	return t.model.Schema
}

func (t *Table) ExecSql(taskName, sql string) (*Instance, error) {
	task := NewSqlTask(taskName, sql, "", nil)
	instances := NewInstances(t.odpsIns, t.ProjectName())
	return instances.CreateTask(t.ProjectName(), &task)
}

func (t *Table) ExecSqlAndWait(taskName, sql string) error {
	instance, err := t.ExecSql(taskName, sql)
	if err != nil {
		return err
	}

	return instance.WaitForSuccess()
}

func (t *Table) AddPartition() error  {

	return nil
}

func (t *TableType) UnmarshalXML(d *xml.Decoder, start xml.StartElement) error {
	var s string

	if err := d.DecodeElement(&s, &start); err != nil {
		return err
	}

	*t = TableTypeFromStr(s)

	return nil
}

func (t TableType) MarshalXML(d *xml.Encoder, start xml.StartElement) error {
	s := t.String()
	return d.EncodeElement(s, start)
}
