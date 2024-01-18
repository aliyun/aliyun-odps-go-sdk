package data

import (
	"fmt"
	"strings"

	"github.com/aliyun/aliyun-odps-go-sdk/odps/datatype"
	"github.com/pkg/errors"
)

type Json struct {
	typ   datatype.JsonType
	data  Data
	Valid bool
}

func NewJsonWithTyp(typ datatype.JsonType) *Json {
	return &Json{
		typ:   typ,
		Valid: true,
	}
}

func (j Json) Type() datatype.DataType {
	return j.typ
}

func (j Json) String() string {
	var sb strings.Builder
	sb.WriteString("JSON '")
	sb.WriteString(j.data.String())
	sb.WriteString("'")

	return sb.String()
}

func (j Json) Sql() string {
	if j.data == nil {
		return "JSON 'NULL'"
	}

	var sb strings.Builder
	sb.WriteString("JSON '")
	sb.WriteString(j.data.Sql())
	sb.WriteString("'")

	return sb.String()
}

func (j *Json) Scan(value interface{}) error {
	return errors.WithStack(tryConvertType(value, j))
}

func (j *Json) SetData(d Data) error {
	if d.Type() != j.typ {
		return errors.New(fmt.Sprintf("Inconsistent data type, expeted: %+v, get: %+v", j.typ.Name(), d.Type().Name()))
	}
	j.data = d
	return nil
}

func (j *Json) GetData() Data {
	return j.data
}
