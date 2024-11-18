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
	"time"

	"github.com/pkg/errors"

	"github.com/aliyun/aliyun-odps-go-sdk/odps/datatype"
)

type IntervalDayTime struct {
	totalSeconds int64
	nanos        int32
}

type IntervalYearMonth int32

func NewIntervalDayTime(totalSeconds int64, nanos int32) IntervalDayTime {
	const NanosPerSecond = int32(time.Second)

	if nanos > 0 {
		totalSeconds += int64(nanos) / int64(NanosPerSecond)
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
	return int32(i.totalSeconds / (24 * 3600))
}

func (i IntervalDayTime) Hours() int32 {
	return int32(i.totalSeconds / 3600)
}

func (i IntervalDayTime) Minutes() int32 {
	return int32(i.totalSeconds / 60)
}

func (i IntervalDayTime) Seconds() int64 {
	return i.totalSeconds
}

func (i IntervalDayTime) SecondsFraction() int32 {
	return int32(i.totalSeconds % (24 * 3600))
}

func (i IntervalDayTime) MillisecondsFraction() int32 {
	return (int32(i.totalSeconds%(24*3600)) * 1000) + i.nanos/int32(time.Millisecond)
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
