package odps

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
}

type Bool bool
type TinyInt int8
type SmallInt int16
type Int int32
type BigInt int64
type Float float32
type Double float64
type Char string
type VarChar string
type String string
type Binary []byte
type Date time.Time
type DateTime time.Time
type Timestamp time.Time

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
type Decimal decimal.Decimal

type StructField struct {
	_type datatype.StructFieldType
	data  Data
}

func NewStructField(_type datatype.StructFieldType, data Data) StructField {
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
