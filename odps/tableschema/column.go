package tableschema

import (
	"encoding/json"
	datatype2 "github.com/aliyun/aliyun-odps-go-sdk/odps/datatype"
	"github.com/pkg/errors"
)

type Column struct {
	Name            string
	Type            datatype2.DataType
	Comment         string
	Label           string
	IsNullable      bool
	HasDefaultValue bool
	DefaultValue    string
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
		return errors.WithStack(err)
	}

	_type, err := datatype2.ParseDataType(cs.Type)
	if err != nil {
		return errors.WithStack(err)
	}

	*c = Column{
		Name:            cs.Name,
		Type:            _type,
		Comment:         cs.Comment,
		Label:           cs.Label,
		IsNullable:      cs.IsNullable,
		HasDefaultValue: cs.HasDefaultValue,
		ExtendedLabels:  cs.ExtendedLabels,
	}

	return nil
}
