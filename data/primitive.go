package data

import (
	"fmt"
	"github.com/aliyun/aliyun-odps-go-sdk/datatype"
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

func (t TinyInt) Type() datatype.DataType {
	return datatype.TinyIntType
}

func (t TinyInt) String() string {
	return fmt.Sprintf("%d", t)
}

func (s SmallInt) Type() datatype.DataType {
	return datatype.SmallIntType
}

func (s SmallInt) String() string {
	return fmt.Sprintf("%d", s)
}

func (i Int) Type() datatype.DataType {
	return datatype.IntType
}

func (i Int) String() string {
	return fmt.Sprintf(  "%d", i)
}

func (b BigInt) Type() datatype.DataType {
	return datatype.BigIntType
}

func (b BigInt) String() string {
	return fmt.Sprintf("%d", b)
}

func (f Float) Type() datatype.DataType {
	return datatype.FloatType
}

func (f Float) String() string {
	return fmt.Sprintf("%E", float32(f))
}

func (d Double) Type() datatype.DataType {
	return datatype.DoubleType
}

func (d Double) String() string {
	return fmt.Sprintf("%E", float64(d))
}

func (s String) Type() datatype.DataType {
	return datatype.StringType
}

func (s String) Value() Data {
	return s
}

func (s String) String() string {
	return string(s)
}

