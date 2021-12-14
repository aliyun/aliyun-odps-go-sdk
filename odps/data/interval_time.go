package data

import (
	"fmt"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/datatype"
	"github.com/pkg/errors"
	"time"
)

type IntervalDayTime struct {
	totalSeconds int32
	nanos        int32
}

type IntervalYearMonth int32

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

func (i IntervalDayTime) Type() datatype.DataType {
	return datatype.IntervalDayTimeType
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

func (i IntervalDayTime) Sql() string {
	return i.String()
}

func (i *IntervalDayTime) Scan(value interface{}) error {
	return errors.WithStack(tryConvertType(value, i))
}

func (i IntervalYearMonth) Type() datatype.DataType {
	return datatype.IntervalYearMonthType
}

func (i IntervalYearMonth) String() string {
	return fmt.Sprintf("%d months", int32(i))
}

func (i IntervalYearMonth) Sql() string {
	return i.String()
}

func (i *IntervalYearMonth) Scan(value interface{}) error {
	return errors.WithStack(tryConvertType(value, i))
}
