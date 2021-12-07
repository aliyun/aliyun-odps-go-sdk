package data

import (
	"github.com/aliyun/aliyun-odps-go-sdk/datatype"
	"time"
)

const (
	DateFormat      = "2006-01-02"
	DateTimeFormat  = "2006-01-02 15:04:05"
	TimeStampFormat = "2006-01-02 15:04:05.000"
)

type Date time.Time
type DateTime time.Time
type Timestamp time.Time

// TODO 仔细查看存入odps和从odps取出的时间有没有差异

func (d Date) Type() datatype.DataType {
	return datatype.DateType
}

func NewDate(s string) (Date, error) {
	t, err := time.Parse(DateFormat, s)
	if err != nil {
		return Date(time.Time{}), err
	}

	return Date(t), nil
}

func (d Date) Value() string {
	return d.String()
}

func (d Date) Time() time.Time {
	return time.Time(d)
}

func (d Date) String() string {
	t := time.Time(d)
	return t.Format(DateFormat)
}

func (d DateTime) Type() datatype.DataType {
	return datatype.DateTimeType
}

func NewDateTime(s string) (DateTime, error) {
	t, err := time.Parse(DateTimeFormat, s)
	if err != nil {
		return DateTime(time.Time{}), err
	}

	return DateTime(t), nil
}

func (d DateTime) Value() string {
	return d.String()
}

func (d DateTime) Time() time.Time {
	return time.Time(d)
}

func (d DateTime) String() string {
	t := time.Time(d)
	return t.Format(DateTimeFormat)
}

func (t Timestamp) Type() datatype.DataType {
	return datatype.TimestampType
}

func NewTimestamp(s string) (Timestamp, error) {
	t, err := time.Parse(TimeStampFormat, s)
	if err != nil {
		return Timestamp(time.Time{}), err
	}

	return Timestamp(t), nil
}

func (t Timestamp) Value() string {
	return t.String()
}

func (t Timestamp) Time() time.Time {
	return time.Time(t)
}

func (t Timestamp) String() string {
	ts := time.Time(t)
	return ts.Format(TimeStampFormat)
}