package data

import (
	"fmt"
	"github.com/aliyun/aliyun-odps-go-sdk/datatype"
	"github.com/pkg/errors"
)

type Bool bool
type TinyInt int8
type Int int32
type SmallInt int16
type BigInt int64
type Float float32
type Double float64
type String string

func (b Bool) Type() datatype.DataType {
	return datatype.BooleanType
}

func (b Bool) String() string {
	return fmt.Sprintf("%t", bool(b))
}

func (b Bool) Sql() string {
	return fmt.Sprintf("%t", bool(b))
}

func (b *Bool) Scan(value interface{}) error {
	return errors.WithStack(tryConvertType(value, b))
}

func (t TinyInt) Type() datatype.DataType {
	return datatype.TinyIntType
}

func (t TinyInt) String() string {
	return fmt.Sprintf("%d", t)
}

func (t TinyInt) Sql() string {
	return fmt.Sprintf("%dY", t)
}

func (t *TinyInt) Scan(value interface{}) error {
	return errors.WithStack(tryConvertType(value, t))
}

func (s SmallInt) Type() datatype.DataType {
	return datatype.SmallIntType
}

func (s SmallInt) String() string {
	return fmt.Sprintf("%d", s)
}

func (s SmallInt) Sql() string {
	return fmt.Sprintf("%ds", s)
}

func (i Int) Type() datatype.DataType {
	return datatype.IntType
}

func (i Int) String() string {
	return fmt.Sprintf("%d", i)
}

func (i Int) Sql() string {
	return fmt.Sprintf("%d", i)
}

func (i *Int) Scan(value interface{}) error {
	return errors.WithStack(tryConvertType(value, i))
}

func (b BigInt) Type() datatype.DataType {
	return datatype.BigIntType
}

func (b BigInt) String() string {
	return fmt.Sprintf("%d", b)
}

func (b BigInt) Sql() string {
	return fmt.Sprintf("%dL", b)
}

func (b *BigInt) Scan(value interface{}) error {
	return errors.WithStack(tryConvertType(value, b))
}

func (f Float) Type() datatype.DataType {
	return datatype.FloatType
}

func (f Float) String() string {
	return fmt.Sprintf("%E", float32(f))
}

func (f Float) Sql() string {
	return fmt.Sprintf("cast(%E as float)", float32(f))
}

func (f *Float) Scan(value interface{}) error {
	return errors.WithStack(tryConvertType(value, f))
}

func (d Double) Type() datatype.DataType {
	return datatype.DoubleType
}

func (d Double) String() string {
	return fmt.Sprintf("%E", float64(d))
}

func (d Double) Sql() string {
	return fmt.Sprintf("%E", float64(d))
}

func (d *Double) Scan(value interface{}) error {
	return errors.WithStack(tryConvertType(value, d))
}

func (s *String) Type() datatype.DataType {
	return datatype.StringType
}

func (s *String) String() string {
	return *((*string)(s))
}

func (s *String) Sql() string {
	return fmt.Sprintf("'%s'", *((*string)(s)))
}

func (s *String) Scan(value interface{}) error {
	return errors.WithStack(tryConvertType(value, s))
}
