package tunnel

import (
	"github.com/aliyun/aliyun-odps-go-sdk/odps/common"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/datatype"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/tableschema"
	"github.com/pkg/errors"
	"net/http"
	"time"
)

func addCommonSessionHttpHeader(header http.Header) {
	header.Add(common.HttpHeaderOdpsDateTransFrom, DateTransformVersion)
	header.Add(common.HttpHeaderOdpsTunnelVersion, Version)
}

type columnResModel struct {
	ColumnId     string `json:"column_id"`
	Comment      string `json:"comment"`
	DefaultValue string `json:"default_value"`
	Name         string `json:"name"`
	Nullable     bool   `json:"nullable"`
	Type         string `json:"type"`
}

type schemaResModel struct {
	IsVirtualView bool             `json:"IsVirtualView"`
	Columns       []columnResModel `json:"columns"`
	PartitionKeys []columnResModel `json:"partitionKeys"`
}

func (s *schemaResModel) toTableSchema(tableName string) (tableschema.TableSchema, error) {
	tableSchema := tableschema.TableSchema{
		TableName:     tableName,
		IsVirtualView: s.IsVirtualView,
	}

	tableSchema.Columns = make([]tableschema.Column, len(s.Columns))
	tableSchema.PartitionColumns = make([]tableschema.Column, len(s.PartitionKeys))

	toOdpsColumn := func(rawColumn columnResModel) (tableschema.Column, error) {
		_type, err := datatype.ParseDataType(rawColumn.Type)
		if err != nil {
			return tableschema.Column{}, errors.WithStack(err)
		}

		column := tableschema.Column{
			Name:         rawColumn.Name,
			Type:         _type,
			Comment:      rawColumn.Comment,
			IsNullable:   rawColumn.Nullable,
			DefaultValue: rawColumn.DefaultValue,
		}

		return column, nil
	}

	for i, rawColumn := range s.Columns {
		column, err := toOdpsColumn(rawColumn)
		if err != nil {
			return tableschema.TableSchema{}, errors.WithStack(err)
		}

		tableSchema.Columns[i] = column
	}

	for i, rawColumn := range s.PartitionKeys {
		column, err := toOdpsColumn(rawColumn)
		if err != nil {
			return tableschema.TableSchema{}, errors.WithStack(err)
		}

		tableSchema.PartitionColumns[i] = column
	}

	return tableSchema, nil
}

func min(x, y int) int {
	if x <= y {
		return x
	}
	return y
}

func Retry(f func() error) {
	sleepTime := int64(1)

	for i := 0; i < 3; i++ {
		err := f()
		if err == nil {
			break
		}

		sleepTime *= 1 << i
		time.Sleep(time.Duration(sleepTime) * time.Second)
	}
}
