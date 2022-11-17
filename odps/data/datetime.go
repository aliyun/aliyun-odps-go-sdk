// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package data

import (
	"fmt"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/datatype"
	"github.com/pkg/errors"
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

func NewDate(s string) (Date, error) {
	t, err := time.ParseInLocation(DateFormat, s, time.Local)
	if err != nil {
		return Date(time.Time{}), err
	}

	return Date(t), nil
}

func (d Date) Type() datatype.DataType {
	return datatype.DateType
}

func (d Date) Time() time.Time {
	return time.Time(d)
}

func (d Date) String() string {
	t := time.Time(d)
	return t.Format(DateFormat)
}

func (d Date) Sql() string {
	return fmt.Sprintf("date'%s'", d.String())
}

func (d *Date) Scan(value interface{}) error {
	return errors.WithStack(tryConvertType(value, d))
}

func NewDateTime(s string) (DateTime, error) {
	t, err := time.ParseInLocation(DateTimeFormat, s, time.Local)
	if err != nil {
		return DateTime(time.Time{}), err
	}

	return DateTime(t), nil
}

func (d DateTime) Type() datatype.DataType {
	return datatype.DateTimeType
}

func (d DateTime) Time() time.Time {
	return time.Time(d)
}

func (d DateTime) String() string {
	t := time.Time(d)
	return t.Format(DateTimeFormat)
}

func (d DateTime) Sql() string {
	return fmt.Sprintf("datetime'%s'", d.String())
}

func (d *DateTime) Scan(value interface{}) error {
	return errors.WithStack(tryConvertType(value, d))
}

func NewTimestamp(s string) (Timestamp, error) {
	t, err := time.ParseInLocation(TimeStampFormat, s, time.Local)
	if err != nil {
		return Timestamp(time.Time{}), err
	}

	return Timestamp(t), nil
}

func (t Timestamp) Type() datatype.DataType {
	return datatype.TimestampType
}

func (t Timestamp) Time() time.Time {
	return time.Time(t)
}

func (t Timestamp) String() string {
	ts := time.Time(t)
	return ts.Format(TimeStampFormat)
}

func (t Timestamp) Sql() string {
	return fmt.Sprintf("timestamp'%s'", t.String())
}

func (t *Timestamp) Scan(value interface{}) error {
	return errors.WithStack(tryConvertType(value, t))
}
