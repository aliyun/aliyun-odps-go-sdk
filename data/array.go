package data

import (
	"fmt"
	"github.com/aliyun/aliyun-odps-go-sdk/datatype"
	"github.com/pkg/errors"
	"strings"
)

type ArrayElementError struct {
	arrayType datatype.ArrayType
	got       datatype.DataType
}

func (a ArrayElementError) Error() string {
	return fmt.Sprintf("try to add %s to %s", a.got.Name(), a.arrayType.Name())
}

type Array struct {
	_type datatype.ArrayType
	data  []Data
}

func (a *Array) AddValue(data ...Data) error {
	et := a._type.ElementType

	for _, d := range data {
		if data == nil {
			a.data = append(a.data, nil)
			continue
		}

		if !datatype.IsTypeEqual(et, d.Type()) {
			return ArrayElementError{
				arrayType: a._type,
				got:       d.Type(),
			}
		}

		a.data = append(a.data, d)
	}

	return nil
}

func (a *Array) Type() datatype.DataType {
	return a._type
}

func (a *Array) Value() []Data {
	return a.data
}

func (a *Array) String() string {
	n := len(a.data)

	if n == 0 {
		return "array()"
	}

	sb := strings.Builder{}
	sb.WriteString("array(")

	for i, d := range a.data {
		sb.WriteString(d.String())

		if i+1 < n {
			sb.WriteString(", ")
		}
	}

	sb.WriteString(")")

	return sb.String()
}

func (a *Array) Len() int {
	return len(a.data)
}

func (a *Array) Index(i int) Data {
	return a.data[i]
}

func (a *Array) Scan(value interface{}) error {
	return errors.WithStack(tryConvertType(value, a))
}

func NewArray(_type datatype.ArrayType) *Array {
	return &Array{
		_type: _type,
	}
}
