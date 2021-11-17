package odps

import (
	"encoding/json"
	"github.com/aliyun/aliyun-odps-go-sdk/datatype"
)

type Column struct {
	Name            string
	Type            datatype.DataType
	Comment         string
	Label           string
	IsNullable      bool
	HasDefaultValue bool
	ExtendedLabels  []string
}

func (c *Column) UnmarshalJSON(data []byte) error {
	type ColumnShadow struct {
		Name            string
		Type            string
		Comment         string
		Label           string
		IsNullable      bool
		HasDefaultValue bool
		ExtendedLabels  []string
	}

	var cs ColumnShadow
	err := json.Unmarshal(data, &cs)
	if err != nil {
		return err
	}

	_type, err := datatype.ParseDataType(cs.Type)
	if err != nil {
		return err
	}

	*c = Column{
		Name: cs.Name,
		Type: _type,
		Comment: cs.Comment,
		Label: cs.Label,
		IsNullable: cs.IsNullable,
		HasDefaultValue: cs.HasDefaultValue,
		ExtendedLabels: cs.ExtendedLabels,
	}

	return nil
}