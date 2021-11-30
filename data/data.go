package data

import (
	"fmt"
	"github.com/aliyun/aliyun-odps-go-sdk/datatype"
	"github.com/shopspring/decimal"
	"time"
)

type Record interface {
}

type Data interface {
	Type() datatype.DataType
	// Value() Data
	fmt.Stringer
}

type Bool bool

func (b Bool) Type() datatype.DataType  {
	return datatype.NewBooleanType()
}

func (b Bool) Value() Data  {
	return b
}

func (b Bool) String() string {
	return fmt.Sprintf("%t", bool(b))
}

type TinyInt int8

func (t TinyInt) Type() datatype.DataType  {
	return datatype.NewTinyintType()
}

func (t TinyInt) Value() Data  {
	return t
}

func (t TinyInt) String() string {
	return fmt.Sprintf("%d", t)
}

type SmallInt int16

func (s SmallInt) Type() datatype.DataType  {
	return datatype.NewSmallintType()
}

func (s SmallInt) Value() Data  {
	return s
}

func (s SmallInt) String() string {
	return fmt.Sprintf("%d", s)
}

type Int int32

func (i Int) Type() datatype.DataType  {
	return datatype.NewIntType()
}

func (i Int) Value() Data  {
	return i
}

func (i Int) String() string {
	return fmt.Sprintf("%d", i)
}

type BigInt int64

func (b BigInt) Type() datatype.DataType  {
	return datatype.NewBigIntType()
}

func (b BigInt) Value() Data  {
	return b
}

func (b BigInt) String() string {
	return fmt.Sprintf("%d", b)
}

type Float float32

func (f Float) Type() datatype.DataType  {
	return datatype.NewFloatType()
}

func (f Float) Value() Data  {
	return f
}

func (f Float) String() string {
	return fmt.Sprintf("%f", f)
}

type Double float64

func (d Double) Type() datatype.DataType  {
	return datatype.NewDoubleType()
}

func (d Double) Value() Data  {
	return d
}

func (d Double) String() string {
	return fmt.Sprintf("%f", d)
}

type Char struct {
	_type datatype.CharType
	value string
}

func (c Char) Type() datatype.DataType  {
	return c._type
}

func (c Char) Value() Data  {
	return c
}

func (c Char) String() string {
	return fmt.Sprintf("%s", c.value)
}

type VarChar struct {
	_type datatype.VarcharType
	value string
}

func (v VarChar) Type() datatype.DataType  {
	return v._type
}

func (v VarChar) Value() Data  {
	return v
}

func (v VarChar) String() string {
	return fmt.Sprintf("%s", v.value)
}

type String string

func (s String) Type() datatype.DataType  {
	return datatype.NewStringType()
}

func (s String) Value() Data  {
	return s
}

func (s String) String() string {
	return fmt.Sprintf("%s", string(s))
}

type Binary []byte

func (b Binary) Type() datatype.DataType  {
	return datatype.NewBinaryType()
}

func (b Binary) Value() Data  {
	return b
}

func (b Binary) String() string {
	return fmt.Sprintf("%+v", []byte(b))
}

type Date time.Time

func (d Date) Type() datatype.DataType  {
	return datatype.NewDateType()
}

func (d Date) Value() Data  {
	return d
}

func (d Date) String() string {
	t := time.Time(d)
	return t.Format("2006-01-02")
}

type DateTime time.Time

func (d DateTime) Type() datatype.DataType  {
	return datatype.NewDateTimeType()
}

func (d DateTime) Value() Data  {
	return d
}

func (d DateTime) String() string {
	t := time.Time(d)
	return t.Format(time.RFC1123)
}

type Timestamp time.Time

func (t Timestamp) Type() datatype.DataType  {
	return datatype.NewTimestampType()
}

func (t Timestamp) Value() Data  {
	return t
}

func (t Timestamp) String() string {
	return fmt.Sprintf("%d", time.Time(t).Unix())
}

type IntervalDayTime struct {
	totalSeconds int32
	nanos        int32
}

func NewIntervalDayTime(totalSeconds int32, nanos int32) IntervalDayTime {
	const NanosPerSecond = int32(time.Second)

	if nanos > 0 {
		totalSeconds += nanos / NanosPerSecond
		nanos %= nanos % NanosPerSecond
	}

	return IntervalDayTime{
		totalSeconds: totalSeconds,
		nanos:        nanos,
	}
}

func (i IntervalDayTime) Type() datatype.DataType  {
	return datatype.NewIntervalDayTimeType()
}

func (i IntervalDayTime) Value() Data  {
	return i
}

func (i IntervalDayTime) Days() int32 {
	return i.totalSeconds / (24 * 3600)
}

func (i IntervalDayTime) Hours() int32 {
	return i.totalSeconds / 3600
}

func (i IntervalDayTime) Minutes() int32 {
	return i.totalSeconds / 60
}

func (i IntervalDayTime) Seconds() int32 {
	return i.totalSeconds
}

func (i IntervalDayTime) SecondsFraction() int32 {
	return i.totalSeconds % (24 * 3600)
}

func (i IntervalDayTime) MillisecondsFraction() int32 {
	return (i.totalSeconds%(24*3600))*1000 + i.nanos/int32(time.Millisecond)
}

func (i IntervalDayTime) NanosFraction() int32 {
	return i.nanos
}

func (i IntervalDayTime) String() string {
	return fmt.Sprintf("%d days, %d ms", i.Days(), i.MillisecondsFraction())
}

type IntervalYearMonth int32

func (i IntervalYearMonth) Type() datatype.DataType  {
	return datatype.NewIntervalYearMonthType()
}

func (i IntervalYearMonth) Value() Data  {
	return i
}

func (i IntervalYearMonth) String() string {
	return fmt.Sprintf("%d months", int32(i))
}

type Decimal struct {
	_type datatype.DecimalType
	value decimal.Decimal
}

func (d Decimal) Type() datatype.DataType  {
	return d._type
}

func (d Decimal) Value() Data  {
	return d
}

func (d Decimal) String() string {
	return d.value.String()
}

type StructField struct {
	_type datatype.StructFieldType
	data  Data
}

func NewStructFieldType(_type datatype.StructFieldType, data Data) StructField {
	return StructField {
		_type: _type,
		data: data,
	}
}


func (f *StructField) Name() string {
	return f._type.Name
}

func (f *StructField) Type() datatype.DataType {
	return f._type.Type
}

func (f *StructField) Data() Data {
	return f.data
}

type Struct struct {
	_type  datatype.StructType
	fields []StructField
}

func NewStruct(_type datatype.StructType, fields []StructField) Struct  {
	return Struct {
		_type: _type,
		fields: fields,
	}
}

type Array struct {
	_type datatype.ArrayType
	data  []Data
}

func (a Array) Type() datatype.ArrayType {
	return a._type
}

func (a Array) Data() []Data {
	return a.data
}

func NewArray(_type datatype.ArrayType, data []Data) Array {
	return Array {
		_type: _type,
		data: data,
	}
}

type Map struct {
	_type datatype.MapType
	data  Data
}