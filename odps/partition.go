package odps

import (
	"encoding/json"
	"encoding/xml"
	"fmt"
	"github.com/pkg/errors"
	"net/url"
	"strings"
	"time"
)

// Partition ODPS分区表中一个特定的分区
type Partition struct {
	odpsIns       *Odps
	projectName   string
	tableName     string
	kv            map[string]string
	model         partitionModel
	extendedModel partitionExtendedModel
}

type partitionModel struct {
	CreateTime         GMTTime `json:"createTime"`
	LastDDLTime        GMTTime `json:"lastDDLTime"`
	LastModifiedTime   GMTTime `json:"lastModifiedTime"`
	PartitionRecordNum int     `json:"partitionRecordNum"`
	PartitionSize      int     `json:"partitionSize"`
}

type partitionExtendedModel struct {
	FileNum      int    `json:"FileNum"`
	IsArchived   bool   `json:"IsArchived"`
	IsExStore    bool   `json:"IsExstore"`
	LifeCycle    int    `json:"LifeCycle"`
	PhysicalSize int    `json:"PhysicalSize"`
	Reserved     string `json:"Reserved"`
}

func NewPartition(odpsIns *Odps, projectName, tableName string, kv map[string]string) Partition {
	return Partition{
		odpsIns:     odpsIns,
		projectName: projectName,
		tableName:   tableName,
		kv:          kv,
	}
}

// Name 返回a=xx,b=yy格式的字符串
func (p *Partition) Name() string {
	i, n := 0, len(p.kv)
	var sb strings.Builder

	for key, value := range p.kv {
		sb.WriteString(fmt.Sprintf("%s='%s'", key, value))
		i += 1

		if i < n {
			sb.WriteString(", ")
		}
	}

	return sb.String()
}

func (p *Partition) Load() error {
	var rb ResourceBuilder
	rb.SetProject(p.projectName)
	resource := rb.Table(p.tableName)
	client := p.odpsIns.restClient

	queryArgs := make(url.Values, 1)
	queryArgs.Set("partition", p.Name())

	type ResModel struct {
		XMLName xml.Name `xml:"Partition"`
		Schema  string
	}

	var resModel ResModel
	err := client.GetWithModel(resource, queryArgs, &resModel)
	if err != nil {
		return errors.WithStack(err)
	}

	var model partitionModel
	err = json.Unmarshal([]byte(resModel.Schema), &model)
	if err != nil {
		return errors.WithStack(err)
	}

	p.model = model

	return nil
}

func (p *Partition) LoadExtended() error {
	var rb ResourceBuilder
	rb.SetProject(p.projectName)
	resource := rb.Table(p.tableName)
	client := p.odpsIns.restClient

	queryArgs := make(url.Values, 1)
	queryArgs.Set("partition", p.Name())
	queryArgs.Set("extended", "")

	type ResModel struct {
		XMLName xml.Name `xml:"Partition"`
		Schema  string
	}

	var resModel ResModel
	err := client.GetWithModel(resource, queryArgs, &resModel)
	if err != nil {
		return errors.WithStack(err)
	}

	var model partitionExtendedModel
	err = json.Unmarshal([]byte(resModel.Schema), &model)
	if err != nil {
		return errors.WithStack(err)
	}

	p.extendedModel = model

	return nil
}

func (p *Partition) CreatedTime() time.Time {
	return time.Time(p.model.CreateTime)
}

// LastDDLTime 分区Meta修改时间
func (p *Partition) LastDDLTime() time.Time {
	return time.Time(p.model.LastDDLTime)
}

func (p *Partition) LastModifiedTime() time.Time {
	return time.Time(p.model.LastModifiedTime)
}

// RecordNum 获取分区数据的Record数，若无准确数据，则返回-1
func (p *Partition) RecordNum() int {
	return p.model.PartitionRecordNum
}

func (p *Partition) Size() int {
	return p.model.PartitionSize
}

func (p *Partition) IsArchivedEx() bool {
	return p.extendedModel.IsArchived
}

func (p *Partition) isExStoreEx() bool {
	return p.extendedModel.IsExStore
}

func (p *Partition) LifeCycleEx() int {
	return p.extendedModel.LifeCycle
}

func (p *Partition) PhysicalSizeEx() int {
	return p.extendedModel.PhysicalSize
}

func (p *Partition) FileNumEx() int {
	return p.extendedModel.FileNum
}

// ReservedEx 返回扩展信息的保留字段 json 字符串
func (p *Partition) ReservedEx() string {
	return p.extendedModel.Reserved
}
