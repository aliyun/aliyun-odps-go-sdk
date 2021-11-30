package tunnel

import (
	odps "github.com/aliyun/aliyun-odps-go-sdk"
	"github.com/aliyun/aliyun-odps-go-sdk/datatype"
	"net/http"
)

func addCommonSessionHttpHeader(header http.Header) {
	header.Add(odps.HttpHeaderOdpsDateTransFrom, DateTransformVersion)
	header.Add(odps.HttpHeaderOdpsTunnelVersion, Version)
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

func (s *schemaResModel) toTableSchema(tableName string) (odps.TableSchema, error)  {
	tableSchema := odps.TableSchema{
		TableName:     tableName,
		IsVirtualView: s.IsVirtualView,
	}

	tableSchema.Columns = make([]odps.Column, len(s.Columns))
	tableSchema.PartitionColumns = make([]odps.Column, len(s.PartitionKeys))

	toOdpsColumn := func(rawColumn columnResModel) (odps.Column, error) {
		_type, err := datatype.ParseDataType(rawColumn.Type)
		if err != nil {
			return odps.Column{}, err
		}

		column := odps.Column{
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
			return odps.TableSchema{}, err
		}

		tableSchema.Columns[i] = column
	}

	for i, rawColumn := range s.PartitionKeys {
		column, err := toOdpsColumn(rawColumn)
		if err != nil {
			return odps.TableSchema{}, err
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