package data

import (
	"fmt"
	"github.com/aliyun/aliyun-odps-go-sdk/datatype"
	"github.com/pkg/errors"
	"strings"
)

type Struct struct {
	_type   datatype.StructType
	data    map[string]Data
	typeMap map[string]datatype.DataType
}

func NewStruct(t datatype.StructType) *Struct {
	typeMap := make(map[string]datatype.DataType)

	for _, f := range t.Fields {
		typeMap[f.Name] = f.Type
	}

	return &Struct{
		_type:   t,
		data:    make(map[string]Data),
		typeMap: typeMap,
	}
}

func (s *Struct) Type() datatype.DataType {
	return s._type
}

func (s *Struct) String() string {
	var sb strings.Builder
	sb.WriteString("struct<")
	n := len(s.data) - 1
	for i, fieldType := range s._type.Fields {
		fieldName := fieldType.Name
		fieldValue := s.data[fieldName]
		sb.WriteString(fieldName)
		sb.WriteString(":")
		sb.WriteString(fieldValue.String())

		if i < n {
			sb.WriteString(",")
		}
	}

	sb.WriteString(">")

	return sb.String()
}

func (s *Struct) Scan(value interface{}) error {
	return errors.WithStack(tryConvertType(value, s))
}

func (s *Struct) GetField(fieldName string) Data {
	return s.data[fieldName]
}

func (s *Struct) SetField(fieldName string, d Data) {
	s.data[fieldName] = d
}

func (s *Struct) FiledType(fieldName string) datatype.DataType {
	return s.typeMap[fieldName]
}

func (s *Struct) Encode(data interface{}) error {
	d, err := TryConvertToOdps(data)
	if err != nil {
		return err
	}

	if !datatype.IsTypeEqual(s._type, d.Type()) {
		return fmt.Errorf("types of the two struct is not equal, %s, %s", d.Type().Name(), s._type.Name())
	}

	return nil
}

var StructDecodeError = errors.New("cannot decode odps struct to go struct")

func (s Struct) Decode(data interface{}) error {

	return nil
}
